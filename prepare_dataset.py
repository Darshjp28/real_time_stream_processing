"""
prepare_dataset.py
-------------------
Converts the real Kaggle IoMT Health Monitoring dataset (XLSX) into a
streaming-ready CSV that Spark can process with event-time windowing.

The original Kaggle dataset has no timestamps or patient IDs.
This script adds them synthetically:
  - 20 patient IDs (P001–P020) rotating across rows
  - Timestamps: one reading every 10 seconds starting 2024-06-01 00:00:00

Usage:
    1. Place patients_data_with_alerts.xlsx in the project root folder
    2. Run: python prepare_dataset.py
    3. Output: data/iomt_health_data.csv (10,000 rows, ready for streaming)
"""

import pandas as pd
import os
from datetime import datetime, timedelta

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
XLSX_INPUT = os.path.join(BASE_DIR, "patients_data_with_alerts.xlsx")
CSV_OUTPUT = os.path.join(BASE_DIR, "data", "iomt_health_data.csv")

NUM_ROWS     = 10_000   # use 10k rows (assignment: 10k–50k is sufficient)
NUM_PATIENTS = 20
START_TIME   = datetime(2024, 6, 1, 0, 0, 0)
INTERVAL_SEC = 10       # one sensor reading every 10 seconds per patient


def main():
    if not os.path.exists(XLSX_INPUT):
        raise FileNotFoundError(
            f"\n[ERROR] Kaggle file not found: {XLSX_INPUT}\n"
            "Download from: https://www.kaggle.com/datasets/anatolii1992/iomt-health-monitoring\n"
            "Place patients_data_with_alerts.xlsx in the project folder."
        )

    print(f"[INFO] Reading {XLSX_INPUT} ...")
    df = pd.read_excel(XLSX_INPUT).head(NUM_ROWS).copy()
    print(f"[INFO] Loaded {len(df):,} rows. Original columns: {list(df.columns)}")

    # ── Add synthetic timestamps (10 s apart) ────────────────────────────────
    df["timestamp"] = [
        (START_TIME + timedelta(seconds=INTERVAL_SEC * i)).strftime("%Y-%m-%d %H:%M:%S")
        for i in range(len(df))
    ]

    # ── Add rotating patient IDs (P001–P020) ─────────────────────────────────
    patient_ids    = [f"P{str(i).zfill(3)}" for i in range(1, NUM_PATIENTS + 1)]
    df["patient_id"] = [patient_ids[i % NUM_PATIENTS] for i in range(len(df))]

    # ── Rename columns to clean snake_case names ──────────────────────────────
    df = df.rename(columns={
        "Heart Rate (bpm)":                "heart_rate",
        "SpO2 Level (%)":                  "spo2",
        "Systolic Blood Pressure (mmHg)":  "systolic_bp",
        "Diastolic Blood Pressure (mmHg)": "diastolic_bp",
        "Body Temperature (°C)":           "temperature",
        "Heart Rate Alert":                "heart_rate_alert",
        "Fall Detection":                  "fall_detection",
        "Predicted Disease":               "predicted_disease",
    })

    # ── Select and reorder final columns ─────────────────────────────────────
    output_cols = [
        "timestamp", "patient_id", "heart_rate", "spo2",
        "systolic_bp", "diastolic_bp", "temperature",
        "heart_rate_alert", "fall_detection", "predicted_disease",
    ]
    df = df[output_cols]

    # ── Save ─────────────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(CSV_OUTPUT), exist_ok=True)
    df.to_csv(CSV_OUTPUT, index=False)

    elevated = (df["heart_rate"] > 100).sum()
    print(f"\n[✓] Saved → {CSV_OUTPUT}")
    print(f"    Rows          : {len(df):,}")
    print(f"    Patients      : {NUM_PATIENTS} (P001–P{str(NUM_PATIENTS).zfill(3)})")
    print(f"    Elevated HR   : {elevated:,} rows ({elevated/len(df)*100:.1f}% > 100 bpm)")
    print(f"    Time range    : {df['timestamp'].iloc[0]}  →  {df['timestamp'].iloc[-1]}")
    print(f"\n[INFO] Ready for streaming. Run: python patient_monitor_stream.py")


if __name__ == "__main__":
    main()
