# ==========================================================
# nse_daily_sync.py
# Author: kriskingg
# Purpose: Maintain full NSE EOD dataset (rate-limited, safe)
# ==========================================================

import os
import time
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from nse import NSE

# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# Adjust days as needed. 180 = 6 months, 365*2 = 2 years.
ROLLING_DAYS = 365 * 2
TODAY = datetime.today()
FROM_DATE = TODAY - timedelta(days=ROLLING_DAYS)

# polite delay between requests (in seconds)
REQUEST_DELAY = 3

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
            time.sleep(REQUEST_DELAY)
            day += timedelta(days=1)

# ----------------------------------------------------------
# MERGE ALL CSVs INTO ONE MASTER FILE
# ----------------------------------------------------------
def merge_bhavcopies():
    # match both old cmDDMMMYYYY and new BhavCopy_NSE_CM_0_0_0_YYYYMMDD format
    files = sorted(DATA_DIR.glob("*CM*.csv"))
    df_list = []

    for f in files:
        try:
            df = pd.read_csv(f)
            # skip empty or malformed
            if df.empty or "TIMESTAMP" not in df.columns:
                print(f"⚠️ Skipped {f.name}: missing TIMESTAMP or empty file")
                continue
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
    print(f"⏳ Using {REQUEST_DELAY}s delay between requests for NSE friendliness.")
    download_bhavcopies(FROM_DATE, TODAY)
    merge_bhavcopies()
