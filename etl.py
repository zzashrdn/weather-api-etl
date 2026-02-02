import os
from dotenv import load_dotenv
from schema import WEATHER_HOURLY_SCHEMA


from functions import (
    setup_logger,
    extract_weather_api,
    archive_raw_json,
    transform_weather_hourly,
    quality_checks,
    save_rejected_csv,
    load_to_sqlite
)

def main():
    load_dotenv()
    logger = setup_logger()

    base_url = os.getenv("API_BASE_URL", "https://api.open-meteo.com/v1/forecast")
    lat = float(os.getenv("LAT", "3.1390"))
    lon = float(os.getenv("LON", "101.6869"))
    db_path = os.getenv("SQLITE_DB", "etl_demo.db")
    tz = os.getenv("TIMEZONE", "Asia/Kuala_Lumpur")

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m",
        "timezone": tz
    }

    # Extract
    raw = extract_weather_api(base_url, params, logger)
    archive_raw_json(raw, "data/raw", "weather_raw", logger)

    # Transform
    df = transform_weather_hourly(raw, logger)


    # Check for Missing 
    missing = [
    c for c in WEATHER_HOURLY_SCHEMA["required_columns"]
    if c not in df.columns

    ]

    if missing:
        raise KeyError(f"Missing required columns: {missing}")


    # Quality + Dedup
    clean, rejected = quality_checks(df, logger)
    save_rejected_csv(rejected, "outputs/rejected_rows.csv", logger)

    # Load
    load_to_sqlite(clean,db_path,WEATHER_HOURLY_SCHEMA["table_name"],logger)


    logger.info("✅ Weather API ETL completed successfully.")

if __name__ == "__main__":
    main()
