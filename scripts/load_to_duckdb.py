import os
import json
import duckdb
import hashlib
from datetime import datetime
from pathlib import Path
import argparse

CONFIG_PATH = "config/series_config.json"
RAW_DATA_DIR = "raw_data"
DB_PATH = "federal_reserve.duckdb"

def load_series_to_duckdb(series_id, conn, batch_loaded_at):
    base = Path(RAW_DATA_DIR) / series_id
    new_records = 0

    conn.execute(f"""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE IF NOT EXISTS raw.{series_id} (
            id_hash TEXT PRIMARY KEY,
            series_id TEXT,
            date DATE,
            value TEXT,
            raw_json JSON,
            loaded_at TIMESTAMP
        );
    """)

    for path in base.rglob("*_observations.json"):
        with open(path) as f:
            raw_data = json.load(f)

        observations = raw_data.get("observations", [])
        for obs in observations:
            if obs.get("value") in (".", None):
                continue  # Skip missing or invalid values
            row = {
                "series_id": series_id,
                "date": obs["date"],
                "value": obs["value"],
                "raw_json": json.dumps(obs),
                "loaded_at": batch_loaded_at
            }

            hash_input = f"{series_id}|{obs['date']}|{obs['value']}"
            row["id_hash"] = hashlib.md5(hash_input.encode()).hexdigest()

            try:
                conn.execute(f"""
                    INSERT INTO raw.{series_id}
                    SELECT ?, ?, ?, ?, ?, ?
                """, (
                    row["id_hash"],
                    row["series_id"],
                    row["date"],
                    row["value"],
                    row["raw_json"],
                    row["loaded_at"]
                ))
                new_records += 1
            except duckdb.ConstraintException:
                pass  # dupes

    print(f"[INFO] Inserted {new_records} records into raw.{series_id}")
    return new_records

def main():
    parser = argparse.ArgumentParser(description="Load FRED series into DuckDB.")
    parser.add_argument("--reset", action="store_true", help="Reset the DuckDB database before loading")
    args = parser.parse_args()

    if args.reset and os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"[INFO] Removed existing {DB_PATH} to recreate fresh database.")

    with open(CONFIG_PATH) as f:
        config = json.load(f)

    batch_loaded_at = datetime.utcnow().isoformat()
    conn = duckdb.connect(DB_PATH)

    for entry in config:
        series_id = entry["series_id"]
        print(f"[INFO] Processing {series_id}...")
        load_series_to_duckdb(series_id, conn, batch_loaded_at)

    conn.close()
    print("[SUCCESS] All series loaded into DuckDB.")

if __name__ == "__main__":
    main()