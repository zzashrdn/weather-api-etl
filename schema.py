WEATHER_HOURLY_SCHEMA = {
    "table_name": "weather_hourly",
    "primary_key": "time",
    "required_columns": [
        "time",
        "temperature_2m",
        "relative_humidity_2m"
    ],
    "numeric_columns": [
        "temperature_2m",
        "relative_humidity_2m"
    ],
    "valid_ranges": {
        "relative_humidity_2m": (0, 100)
    }
}
