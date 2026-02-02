
import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, Any, Tuple
import pandas as pd
import requests
from sqlalchemy import create_engine


# Logging/Utils
def setup_logger(log_path: str = "outputs/etl.log") -> logging.Logger:
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logger = logging.getLogger("etl")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


def archive_raw_json(data: Dict[str, Any], raw_dir: str, prefix: str, logger: logging.Logger) -> str:
    os.makedirs(raw_dir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(raw_dir, f"{prefix}_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"Extract: archived raw JSON -> {path}")
    return path


def save_rejected_csv(df: pd.DataFrame, path: str, logger: logging.Logger) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"Audit: rejected rows saved -> {path}")



# Extract (API)
def extract_weather_api(
    base_url: str,
    params: Dict[str, Any],
    logger: logging.Logger,
    timeout: int = 30,
    retries: int = 3
) -> Dict[str, Any]:
    """
    Simple retry logic (intern-project feel).
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Extract(API): GET {base_url} attempt={attempt} params={params}")
            r = requests.get(base_url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.warning(f"Extract(API): failed attempt={attempt} error={e}")
            if attempt == retries:
                raise
            time.sleep(2 * attempt)


# Transform
def transform_weather_hourly(data: Dict[str, Any], logger: logging.Logger) -> pd.DataFrame:
    """
    Converts Open-Meteo hourly JSON into a clean dataframe.
    """
    hourly = data.get("hourly", {})
    df = pd.DataFrame({
        "time": hourly.get("time", []),
        "temperature_2m": hourly.get("temperature_2m", []),
        "relative_humidity_2m": hourly.get("relative_humidity_2m", []),
    })

    # Type casting
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["temperature_2m"] = pd.to_numeric(df["temperature_2m"], errors="coerce")
    df["relative_humidity_2m"] = pd.to_numeric(df["relative_humidity_2m"], errors="coerce")

    logger.info(f"Transform: rows={len(df)} cols={list(df.columns)}")
    return df



# Quality Checks and Dedup
def quality_checks(df: pd.DataFrame, logger: logging.Logger) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Rules:
      - time must not be null
      - humidity must be between 0 and 100
    Dedup:
      - keep first row per time
    """
    ok = df["time"].notna() & df["relative_humidity_2m"].between(0, 100, inclusive="both")

    rejected = df[~ok].copy()
    clean = df[ok].copy()

    before = len(clean)
    clean = clean.drop_duplicates(subset=["time"], keep="first")

    logger.info(f"Quality: accepted={len(clean)} rejected={len(rejected)} dedup_removed={before - len(clean)}")
    return clean, rejected

# Load
def load_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str, logger: logging.Logger) -> None:
    engine = create_engine(f"sqlite:///{db_path}")
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    logger.info(f"Load: wrote {len(df)} rows -> table={table_name} db={db_path}")
