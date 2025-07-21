import os
import json
import requests
import click
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("FRED_API_KEY")
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

def extract():
    from_date = "2000-01-01"
    to_date = datetime.today().strftime("%Y-%m-%d")

    print(f"[INFO] Extracting data from {from_date} to {to_date}")

    with open("config/series_config.json") as f:
        series_list = json.load(f)

    for s in series_list:
        series_id = s["series_id"]
        print(f"[INFO] Fetching data for {series_id}...")

        params = {
            "series_id": series_id,
            "api_key": API_KEY,
            "file_type": "json",
            "observation_start": from_date,
            "observation_end": to_date
        }

        response = requests.get(BASE_URL, params=params)
        data = response.json()

        if "observations" not in data:
            print(f"[WARN] No observations found for {series_id}")
            continue

        today = datetime.today()
        dir_path = Path(f"raw_data/{series_id}/{today:%Y}/{today:%m}/{today:%d}")
        dir_path.mkdir(parents=True, exist_ok=True)

        today_str = datetime.today().strftime("%Y%m%d")
        filename = f"{series_id}{today_str}_observations.json"

        with open(dir_path / filename, "w") as f:
            json.dump(data, f, indent=2)

        print(f"[SUCCESS] Stored data for {series_id} in {dir_path}")

if __name__ == "__main__":
    extract()