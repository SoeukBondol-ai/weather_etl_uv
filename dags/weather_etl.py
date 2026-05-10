import json
import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Column, DateTime, Integer, Numeric, String, Text
from sqlalchemy.orm import declarative_base, sessionmaker

log = logging.getLogger(__name__)

# ── SQLAlchemy Declarative Model ──────────────────────────────────────────────
Base = declarative_base()

class WeatherData(Base):
    __tablename__ = "weather_data"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    city        = Column(String(100), nullable=False)
    temp_c      = Column(Numeric(5, 1))
    feels_like  = Column(Numeric(5, 1))
    humidity    = Column(Integer)
    pressure    = Column(Integer)
    wind_speed  = Column(Numeric(5, 2))
    description = Column(Text)
    heat_index  = Column(String(20))
    recorded_at = Column(DateTime)
    loaded_at   = Column(DateTime, default=datetime.utcnow)


# ── Config from environment variables ──────────────────────────────────────────
API_KEY    = os.getenv("OPENWEATHER_API_KEY", "YOUR_API_KEY")
CITY       = os.getenv("WEATHER_CITY", "Bangkok")
RAW_PATH   = "/opt/airflow/data/weather_raw.json"
CLEAN_PATH = "/opt/airflow/data/weather_clean"
DB_CONN    = "postgresql+psycopg2://weather:weather@postgres/weather_db"

# ── Default task arguments ─────────────────────────────────────────────────────
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ══════════════════════════════════════════════════════════════════════════════
# TASK 1 — EXTRACT
# ══════════════════════════════════════════════════════════════════════════════
def extract_weather(**kwargs):
    """Fetch weather data from OpenWeatherMap and save as JSON."""
    os.makedirs(os.path.dirname(RAW_PATH), exist_ok=True)

    # Check if API key is unconfigured
    is_placeholder_key = not API_KEY or API_KEY in ["YOUR_OPENWEATHER_API_KEY", "YOUR_API_KEY", ""]

    if is_placeholder_key:
        log.warning(
            "No valid OpenWeatherMap API key found. "
            "Falling back to simulated weather data."
        )
        write_simulated_weather(**kwargs)
        return

    url = (
        "https://api.openweathermap.org/data/2.5/weather"
        f"?q={CITY}&appid={API_KEY}&units=metric"
    )
    log.info(f"Fetching weather for city: {CITY}")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        with open(RAW_PATH, "w") as f:
            json.dump(data, f, indent=2)

        log.info(f"Raw data saved to {RAW_PATH}")
        log.info(f"  City     : {data.get('name')}")
        log.info(f"  Temp     : {data['main']['temp']} °C")
        log.info(f"  Humidity : {data['main']['humidity']} %")
    except Exception as e:
        log.warning(
            f"Failed to fetch real weather data due to: {e}. "
            "Falling back to simulated weather data."
        )
        write_simulated_weather(**kwargs)


def write_simulated_weather(**kwargs):
    """Generate realistic simulated weather data for testing."""
    import random
    import time

    simulated_data = {
        "coord": {"lon": 100.5167, "lat": 13.75},
        "weather": [
            {
                "id": 801,
                "main": "Clouds",
                "description": "few clouds",
                "icon": "02d"
            }
        ],
        "base": "stations",
        "main": {
            "temp": round(random.uniform(28.0, 36.0), 1),
            "feels_like": round(random.uniform(31.0, 42.0), 1),
            "temp_min": 27.0,
            "temp_max": 37.0,
            "pressure": 1008,
            "humidity": random.randint(55, 85)
        },
        "visibility": 10000,
        "wind": {
            "speed": round(random.uniform(1.5, 6.0), 1),
            "deg": 180
        },
        "clouds": {
            "all": 20
        },
        "dt": int(time.time()),
        "sys": {
            "type": 1,
            "id": 8183,
            "country": "TH",
            "sunrise": int(time.time()) - 10000,
            "sunset": int(time.time()) + 10000
        },
        "timezone": 25200,
        "id": 1609350,
        "name": CITY,
        "cod": 200
    }

    with open(RAW_PATH, "w") as f:
        json.dump(simulated_data, f, indent=2)

    log.info(f"Simulated raw data saved to {RAW_PATH}")
    log.info(f"  City     : {simulated_data.get('name')}")
    log.info(f"  Temp     : {simulated_data['main']['temp']} °C")
    log.info(f"  Humidity : {simulated_data['main']['humidity']} %")

    # Push file path to XCom if ti is present in kwargs
    if "ti" in kwargs:
        kwargs["ti"].xcom_push(key="raw_path", value=RAW_PATH)
    return RAW_PATH


# ══════════════════════════════════════════════════════════════════════════════
# TASK 3 — LOAD  (Task 2 is the SparkSubmitOperator defined in the DAG below)
# ══════════════════════════════════════════════════════════════════════════════
def load_to_postgres(**kwargs):
    """Read the cleaned Parquet written by Spark and insert into PostgreSQL via SQLAlchemy."""

    import glob

    # Find all parquet part files written by Spark
    parquet_files = glob.glob(f"{CLEAN_PATH}/*.parquet")
    if not parquet_files:
        raise FileNotFoundError(
            f"No parquet files found at {CLEAN_PATH}. "
            "Check that the Spark transform task succeeded."
        )

    log.info(f"Found {len(parquet_files)} parquet file(s) to load")

    df = pd.read_parquet(CLEAN_PATH)
    log.info(f"Loaded {len(df)} row(s) from parquet")
    log.info(f"Columns: {list(df.columns)}")

    # 1. Fetch SQLAlchemy engine from Airflow's PostgresHook
    log.info("Connecting to Postgres via PostgresHook...")
    hook = PostgresHook(postgres_conn_id="postgres_weather")
    engine = hook.get_sqlalchemy_engine()

    # 2. Convert DataFrame rows into dictionary mappings for bulk insert
    records = df.to_dict(orient="records")

    # 3. Perform high-performance bulk insert using SQLAlchemy session
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        log.info("Performing bulk insert via SQLAlchemy Session...")
        session.bulk_insert_mappings(WeatherData, records)
        session.commit()
        log.info(f"Successfully loaded {len(records)} row(s) into weather_data via SQLAlchemy")
    except Exception as e:
        session.rollback()
        log.error(f"Failed to load rows into PostgreSQL: {e}")
        raise
    finally:
        session.close()
        engine.dispose()

    return len(records)


# ══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    description="Daily weather ETL: API → Spark → PostgreSQL",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",   # daily at 06:00 UTC
    catchup=False,
    tags=["weather", "etl", "spark", "postgres"],
) as dag:

    # ── Task 1: Extract ──────────────────────────────────────────────────────
    extract_task = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
        provide_context=True,
    )

    # ── Task 2: Transform (Spark) ────────────────────────────────────────────
    transform_task = SparkSubmitOperator(
        task_id="transform_with_spark",
        application="/opt/airflow/spark/transform.py",
        conn_id="spark_default",
        application_args=[RAW_PATH, CLEAN_PATH],
        verbose=False,
        conf={
            "spark.driver.memory": "512m",
        },
    )

    # ── Task 3: Load ─────────────────────────────────────────────────────────
    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        provide_context=True,
    )

    # ── Dependencies: extract → transform → load ─────────────────────────────
    extract_task >> transform_task >> load_task
