# federal reserve etl pipeline

Data solution developed to extract, store, model and make available economic series from the US Federal Reserve using the FRED API.

---

##  Main features

* Time series extraction from FRED
* Storage in `.json` files organized by `series_id`.
* Structured ingestion on DuckDB basis (`raw` schema)
* Transformation and incremental modeling with dbt (`clean` and `datawarehouse` schemes)
* Export final report as `.csv` file

---

## Technologies used

* **Python 3.11+**
* **DuckDB** as a local database
* **dbt** for data modeling and transformation
* **.env** for credential management (FRED API Key)

---

##  Requirements

* Python >= 3.11
* `git` y `pip`
* Compatible with Windows, macOS or Linux

---

##  Installation

```bash
git clone https://github.com/NicoPare/finalis_etl.git
cd finalis_etl

python -m venv .venv
source .venv/bin/activate        # Mac/Linux
.venv\Scripts\activate           # Windows

pip install -r requirements.txt
```

---

##  API Key Configuration

1. Register in [https://fred.stlouisfed.org/](https://fred.stlouisfed.org/)
2. Generate your API Key from your profile
3. Create an `.env` file with this content:

```
FRED_API_KEY=YOUR_API_KEY_HERE
```

An example is found in `.env.example`.

---

## Execute the complete pipeline

```bash
python run_pipeline.py           # incremental load
python run_pipeline.py --reset  # Delete and reload everything since 2000-01-01
```

This executes, in order:

* Extraction from the API
* Ingest into DuckDB
* Modeling with dbt (only models with `federal_reserve` tag)
* Report export as CSV

The final report is stored in `reports/YYYYMMDD/federal_reserve_report_dataset_{timestamp}.csv`

---