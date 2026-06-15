"""
stream_feeder.py
-----------------
Simulates a live ICU sensor stream by writing small CSV batches into
data/input/ every few seconds. Spark's readStream (watched directory)
picks up each new file as a streaming micro-batch.

Usage (Terminal 2, AFTER starting patient_monitor_stream.py):
    python stream_feeder.py

You will see batches being written and Spark processing them in Terminal 1.
"""

import csv
import os
import time
from datetime import datetime

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
SOURCE_CSV = os.path.join(BASE_DIR, "data", "iomt_health_data.csv")
INPUT_DIR  = os.path.join(BASE_DIR, "data", "input")

# ── Settings ─────────────────────────────────────────────────────────────────
BATCH_SIZE  = 50    # rows per CSV file (50 rows × 10s interval = ~8 min of data per file)
SLEEP_SECS  = 5     # seconds between file drops

FIELDNAMES = [
    "timestamp", "patient_id", "heart_rate", "spo2",
    "systolic_bp", "diastolic_bp", "temperature",
    "heart_rate_alert", "fall_detection", "predicted_disease",
]


def main():
    if not os.path.exists(SOURCE_CSV):
        raise FileNotFoundError(
            f"\n[ERROR] Dataset not found: {SOURCE_CSV}\n"
            "Run prepare_dataset.py first to generate it from the Kaggle XLSX."
        )

    os.makedirs(INPUT_DIR, exist_ok=True)

    # Load all rows from the prepared dataset
    with open(SOURCE_CSV, newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    total = len(all_rows)
    print(f"\n{'='*60}")
    print(f"  Stream Feeder — IoMT Patient Monitoring")
    print(f"  Dataset : {total:,} rows")
    print(f"  Batch   : {BATCH_SIZE} rows/file every {SLEEP_SECS}s")
    print(f"  Output  : {INPUT_DIR}")
    print(f"{'='*60}\n")

    batch_num = 0
    offset    = 0

    while offset < total:
        batch      = all_rows[offset : offset + BATCH_SIZE]
        offset    += BATCH_SIZE
        batch_num += 1

        filename = os.path.join(INPUT_DIR, f"batch_{batch_num:05d}.csv")

        with open(filename, "w", newline="", encoding="utf-8") as out:
            writer = csv.DictWriter(out, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(batch)

        # Stats for this batch
        elevated = sum(1 for r in batch if int(r["heart_rate"]) > 100)
        ts_start = batch[0]["timestamp"]
        ts_end   = batch[-1]["timestamp"]

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}]  "
            f"batch_{batch_num:05d}.csv  |  "
            f"rows={len(batch):3d}  |  "
            f"elevated_hr={elevated:3d}  |  "
            f"data: {ts_start} → {ts_end}"
        )

        time.sleep(SLEEP_SECS)

    print(f"\n[INFO] All {total:,} rows fed into stream.")
    print("[INFO] Keeping process alive so Spark can finish processing ...")
    print("[INFO] Press Ctrl+C to exit.\n")

    while True:
        time.sleep(30)


if __name__ == "__main__":
    main()
