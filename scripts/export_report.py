import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path

db_path = "federal_reserve.duckdb"
output_dir = Path("reports") / datetime.today().strftime("%Y%m%d")
output_dir.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"federal_reserve_report_dataset_{timestamp}.csv"
output_path = output_dir / filename

con = duckdb.connect(db_path)
df = con.sql("SELECT * FROM main.federal_reserve_observations ORDER BY observation_date DESC").fetchdf()
df.to_csv(output_path, index=False)

print(f"[SUCCESS] Exported final report to {output_path}")
