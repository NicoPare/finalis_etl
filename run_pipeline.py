import subprocess
import os
import argparse



def run(command):
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        exit(1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Reset DuckDB before load")
    args = parser.parse_args()

    # Moverse al directorio raíz del proyecto (finalis_etl/)
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    db_path = os.path.abspath("federal_reserve.duckdb")
    os.environ["DUCKDB_PATH"] = db_path

    run("python scripts/extract_data.py")
    run(f"python scripts/load_to_duckdb.py{' --reset' if args.reset else ''}")
    run("dbt run --project-dir dbt --profiles-dir dbt --select tag:federal_reserve")
    run("python scripts/export_report.py")

if __name__ == "__main__":
    main()
