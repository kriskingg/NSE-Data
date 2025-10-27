# ==========================================================
# nse_daily_sync.py
# Author: kriskingg
# Purpose: Maintain rolling NSE EOD dataset (fast test mode)
# ==========================================================

import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from nse import NSE

# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

ROLLING_DAYS = 5  # Fetch only last 5 days for testing (adjust later)
TODAY = datetime.today()
FROM_DATE = TODAY - timedelta(days=ROLLING_DAYS)

# ----------------------------------------------------------
# DOWNLOAD FUNCTION
# ----------------------------------------------------------
def download_bhavcopies(start: datetime, end: datetime):
    with NSE(download_folder=DATA_DIR, server=True) as nse:
        day = start
        while day <= end:
            try:
                file_path = nse.equityBhavcopy(day, DATA_DIR)
                print(f"✅ {day.strftime('%Y-%m-%d')} → {file_path.name}")
            except Exception as e:
                print(f"⚠️ {day.strftime('%Y-%m-%d')}: {e}")
            day += timedelta(days=1)

# ----------------------------------------------------------
# MERGE ALL CSVs INTO ONE MASTER FILE
# ----------------------------------------------------------
def merge_bhavcopies():
    files = sorted(DATA_DIR.glob("cm*.csv"))
    df_list = []

    for f in files:
        try:
            df = pd.read_csv(f)
            df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
            df_list.append(df)
        except Exception as e:
            print(f"⚠️ Skipped {f.name}: {e}")

    if not df_list:
        print("❌ No data files found.")
        return

    full_df = pd.concat(df_list, ignore_index=True)
    full_df = full_df[
        ["SYMBOL", "SERIES", "OPEN", "HIGH", "LOW", "CLOSE", "TOTTRDQTY", "TOTTRDVAL", "TIMESTAMP"]
    ]
    full_df.sort_values(["SYMBOL", "TIMESTAMP"], inplace=True)
    output_file = DATA_DIR / "nse_daily_master.csv"
    full_df.to_csv(output_file, index=False)
    print(f"✅ Master CSV saved: {output_file}")
    print(full_df.tail(5))

# ----------------------------------------------------------
# MAIN EXECUTION
# ----------------------------------------------------------
if __name__ == "__main__":
    print(f"🔁 Updating NSE dataset from {FROM_DATE.strftime('%Y-%m-%d')} → {TODAY.strftime('%Y-%m-%d')}")
    download_bhavcopies(FROM_DATE, TODAY)
    merge_bhavcopies()
