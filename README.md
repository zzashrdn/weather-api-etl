# Weather API ETL (Open-Meteo), Portfolio Project

A mini ETL pipeline that extracts hourly weather data from the Open-Meteo API, archives raw JSON, transforms it into a clean tabular dataset, applies data quality checks, and loads results into a SQLite database.

## Features
- Extract from public API (no API key needed)
- Raw JSON archiving for audit (`data/raw/`)
- Transformation to clean dataframe (`time`, `temperature_2m`, `relative_humidity_2m`)
- Data Quality rules:
  - `time` must be valid
  - `relative_humidity_2m` must be between 0 and 100
  - deduplicate by `time`
- Audit output: rejected rows saved to `outputs/rejected_rows.csv`
- Logging to console + `outputs/etl.log`
- Load to SQLite table: `weather_hourly`

## Project Structure
- `etl.py` orchestrates the pipeline
- `functions.py` contains Extract / Transform / Quality / Load utilities

## Setup
```bash
python -m venv venv
# Windows:
venv\Scripts\activate
pip install -r requirements.txt
