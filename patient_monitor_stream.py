"""
=============================================================================
ENGR 5785G: Real-time Data Analytics for IoT
Assignment : Real-Time Stream Processing
Scenario   : B – Hospital Patient Monitoring
Student    : [Your Name]
=============================================================================

OBJECTIVE:
    Detect sustained abnormal heart rates across ICU patient streams using
    Spark Structured Streaming with a Tumbling 2-minute window.

DATASET:
    IoMT Health Monitoring Dataset (Kaggle)
    File: patients_data_with_alerts.xlsx → preprocessed to data/iomt_health_data.csv
    Rows used: 10,000 (assignment requires 10,000–50,000)

WINDOW TYPE: Tumbling 2-minute window
    WHY: A tumbling window divides time into non-overlapping, fixed-size
    intervals. This is ideal for detecting PERSISTENCE of a condition —
    if average HR exceeds 100 bpm in two back-to-back tumbling windows,
    it means the condition lasted at least 4 continuous minutes.
    A sliding window would reuse readings across overlapping windows,
    causing false positives from single spikes. A session window does not
    apply here because ICU sensors produce continuous, periodic readings
    with no natural activity gaps.

WHERE STATE IS REQUIRED:
    1. IMPLICIT STATE (window aggregation):
       Spark maintains partial aggregates (running sum + count for avg HR)
       per patient per window internally in its state store.
       withWatermark("event_time","1 minute") controls when this state is
       evicted after the window closes.

    2. EXPLICIT STATE (flatMapGroupsWithState):
       To check if TWO CONSECUTIVE windows are both elevated, the pipeline
       must remember the previous window's alert flag per patient.
       GroupState stores (prev_window_start, prev_was_elevated) per patient.
       Without this state, each batch is independent and consecutive-window
       detection is impossible.

HOW TO RUN:
    Terminal 1: python patient_monitor_stream.py
    Terminal 2: python stream_feeder.py
=============================================================================
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
    TimestampType, BooleanType,
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from typing import Iterator, Tuple

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR       = os.path.join(BASE_DIR, "data", "input")        # readStream watches here
OUTPUT_DIR      = os.path.join(BASE_DIR, "data", "output")       # alert CSV sink
CHECKPOINT_DIR  = os.path.join(BASE_DIR, "data", "checkpoint")   # Spark checkpoint

WINDOW_DURATION = "2 minutes"    # Tumbling window size (Scenario B requirement)
WATERMARK_DELAY = "1 minute"     # Tolerate late-arriving data up to 1 min
ALERT_THRESHOLD = 100            # Heart rate threshold (bpm) for clinical alert
TRIGGER_SECS    = 30             # Micro-batch trigger interval

# =============================================================================
# SCHEMA — matches data/iomt_health_data.csv (preprocessed from Kaggle XLSX)
# =============================================================================

RAW_SCHEMA = StructType([
    StructField("timestamp",         StringType(),  True),   # "2024-06-01 00:00:00"
    StructField("patient_id",        StringType(),  True),   # "P001" … "P020"
    StructField("heart_rate",        IntegerType(), True),   # bpm  (key metric)
    StructField("spo2",              IntegerType(), True),   # SpO2 %
    StructField("systolic_bp",       IntegerType(), True),   # mmHg
    StructField("diastolic_bp",      IntegerType(), True),   # mmHg
    StructField("temperature",       DoubleType(),  True),   # °C
    StructField("heart_rate_alert",  StringType(),  True),   # "Normal" / "High" (ground truth)
    StructField("fall_detection",    StringType(),  True),   # "Yes" / "No"
    StructField("predicted_disease", StringType(),  True),   # disease label
])

# =============================================================================
# OUTPUT SCHEMA for flatMapGroupsWithState
# =============================================================================

WINDOW_STATE_SCHEMA = StructType([
    StructField("patient_id",      StringType(),    False),
    StructField("window_start",    TimestampType(), False),
    StructField("window_end",      TimestampType(), False),
    StructField("avg_heart_rate",  DoubleType(),    False),
    StructField("reading_count",   IntegerType(),   False),
    StructField("max_heart_rate",  IntegerType(),   False),
    StructField("this_elevated",   BooleanType(),   False),  # this window > 100 bpm?
    StructField("prev_elevated",   BooleanType(),   False),  # previous window > 100 bpm?
    StructField("sustained_alert", BooleanType(),   False),  # BOTH elevated → alert!
])

# =============================================================================
# STATEFUL FUNCTION — tracks consecutive elevated windows per patient
# STATE: (prev_window_start_str: str, prev_was_elevated: bool)
# =============================================================================

def detect_sustained_alert(
    patient_id: str,
    rows: Iterator,
    state: GroupState,
) -> Iterator[Tuple]:
    """
    Called once per patient per micro-batch with all new window results for
    that patient. Uses GroupState to remember whether the PREVIOUS window
    was elevated (avg HR > 100 bpm).

    Alert fires only when BOTH this_elevated AND prev_elevated are True
    — i.e., two consecutive 2-minute windows above threshold.
    """
    # Retrieve previous window state for this patient
    if state.exists:
        prev_window_start, prev_elevated = state.get
    else:
        prev_window_start, prev_elevated = None, False

    for row in rows:
        avg_hr        = float(row.avg_heart_rate)
        this_elevated = avg_hr > ALERT_THRESHOLD
        # ALERT: both current AND previous window above threshold
        sustained     = this_elevated and prev_elevated

        # Update state: remember this window for next batch
        state.update((str(row.window_start), this_elevated))
        # Expire state if patient goes silent for 10 minutes
        state.setTimeoutDuration(10 * 60 * 1000)

        yield (
            patient_id,
            row.window_start,
            row.window_end,
            round(avg_hr, 2),
            int(row.reading_count),
            int(row.max_heart_rate),
            this_elevated,
            prev_elevated,
            sustained,      # ← the alert flag
        )


# =============================================================================
# MAIN STREAMING PIPELINE
# =============================================================================

def main():
    # Ensure all directories exist
    for d in [INPUT_DIR, OUTPUT_DIR, CHECKPOINT_DIR]:
        os.makedirs(d, exist_ok=True)

    # ── Spark Session ─────────────────────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName("ENGR5785G_ScenarioB_PatientMonitoring")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        # Disable strict stateful correctness check (needed for flatMapGroupsWithState
        # used alongside window aggregation in append mode)
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 70)
    print("  ENGR 5785G  |  Scenario B: Hospital Patient Monitoring")
    print("  Dataset    : IoMT Health Monitoring (Kaggle) — 10,000 rows")
    print(f"  Window     : Tumbling {WINDOW_DURATION} per patient")
    print(f"  Alert      : avg HR > {ALERT_THRESHOLD} bpm in 2 consecutive windows")
    print("=" * 70 + "\n")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 1: readStream — watch INPUT_DIR for new CSV files
    # Simulates a live sensor stream; each file = one batch of readings
    # ─────────────────────────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .schema(RAW_SCHEMA)
        .option("header", "true")
        .option("maxFilesPerTrigger", "5")      # process up to 5 files per micro-batch
        .csv(INPUT_DIR)
    )

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 2: Parse event_time, filter null/bad rows
    # ─────────────────────────────────────────────────────────────────────────
    parsed = (
        raw_stream
        .withColumn(
            "event_time",
            F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")
        )
        .filter(F.col("patient_id").isNotNull())
        .filter(F.col("heart_rate").isNotNull())
        .filter(F.col("event_time").isNotNull())
    )

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 3: Tumbling 2-minute window aggregation WITH watermark
    #
    # withWatermark: tells Spark to tolerate data arriving up to 1 minute late.
    #   State for window W is evicted only after:
    #   max_seen_event_time − watermark_delay > window_end
    #
    # groupBy(window(...), patient_id): tumbling window (no slide arg)
    #   Each reading falls into exactly ONE window. Non-overlapping.
    # ─────────────────────────────────────────────────────────────────────────
    windowed_agg = (
        parsed
        .withWatermark("event_time", WATERMARK_DELAY)           # ← STATE point 1
        .groupBy(
            F.window("event_time", WINDOW_DURATION),            # tumbling: no 2nd arg
            F.col("patient_id"),
        )
        .agg(
            F.avg("heart_rate").alias("avg_heart_rate"),
            F.count("*").alias("reading_count"),
            F.max("heart_rate").alias("max_heart_rate"),
            F.min("heart_rate").alias("min_heart_rate"),
        )
        .select(
            F.col("patient_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("avg_heart_rate"),
            F.col("reading_count"),
            F.col("max_heart_rate"),
            F.col("min_heart_rate"),
        )
    )

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4: flatMapGroupsWithState — explicit state for consecutive windows
    #
    # Groups results by patient_id. For each patient, calls
    # detect_sustained_alert() which remembers the previous window's flag.
    # ─────────────────────────────────────────────────────────────────────────
    with_state = (
        windowed_agg                                            # ← STATE point 2
        .groupBy("patient_id")
        .flatMapGroupsWithState(
            outputMode="append",
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
            func=detect_sustained_alert,
            outputStructType=WINDOW_STATE_SCHEMA,
        )
    )

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 5: Filter — keep only SUSTAINED ALERTS (both windows elevated)
    # This is the required "alert condition as a filtered output stream"
    # ─────────────────────────────────────────────────────────────────────────
    clinical_alerts = (
        with_state
        .filter(F.col("sustained_alert") == True)
        .select(
            F.lit(">>> CLINICAL ALERT <<<").alias("alert_type"),
            F.col("patient_id"),
            F.col("window_start"),
            F.col("window_end"),
            F.round("avg_heart_rate", 1).alias("avg_hr_bpm"),
            F.col("reading_count"),
            F.col("max_heart_rate").alias("max_hr_bpm"),
            F.lit(f"Avg HR > {ALERT_THRESHOLD} bpm sustained across 2 consecutive 2-min windows")
             .alias("reason"),
        )
    )

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 6: Output Sinks
    # ─────────────────────────────────────────────────────────────────────────

    # Sink A: Console — all window aggregation results (monitoring view)
    windowed_agg.writeStream \
        .queryName("all_window_results") \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "30") \
        .trigger(processingTime=f"{TRIGGER_SECS} seconds") \
        .start()

    # Sink B: Console — ALERTS ONLY (required alert output for screenshot)
    clinical_alerts.writeStream \
        .queryName("clinical_alerts_console") \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime=f"{TRIGGER_SECS} seconds") \
        .start()

    # Sink C: CSV file — persist all fired alerts to disk
    clinical_alerts.writeStream \
        .queryName("clinical_alerts_csv") \
        .outputMode("append") \
        .format("csv") \
        .option("path", OUTPUT_DIR) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .option("header", "true") \
        .trigger(processingTime=f"{TRIGGER_SECS} seconds") \
        .start()

    print(f"[INFO] Watching  : {INPUT_DIR}")
    print(f"[INFO] Alerts CSV: {OUTPUT_DIR}")
    print(f"[INFO] Trigger   : every {TRIGGER_SECS} seconds")
    print("[INFO] Run in Terminal 2: python stream_feeder.py\n")
    print("Waiting for data ...\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down streaming queries ...")
        for q in spark.streams.active:
            q.stop()
        print("[INFO] All queries stopped. Goodbye.")


if __name__ == "__main__":
    main()
