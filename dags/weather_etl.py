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
from sqlalchemy import Boolean, Column, DateTime, Integer, Numeric, String, Text, desc
from sqlalchemy.orm import declarative_base, sessionmaker

log = logging.getLogger(__name__)

# -- SQLAlchemy Declarative Model ----------------------------------------------
Base = declarative_base()

class WeatherData(Base):
    __tablename__ = "weather_data"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    city         = Column(String(100), nullable=False)
    temp_c       = Column(Numeric(5, 1))
    feels_like   = Column(Numeric(5, 1))
    humidity     = Column(Integer)
    pressure     = Column(Integer)
    wind_speed   = Column(Numeric(5, 2))
    clouds       = Column(Integer)
    description  = Column(Text)
    heat_index   = Column(String(20))
    aqi          = Column(Integer)
    pm2_5        = Column(Numeric(6, 2))
    pm10         = Column(Numeric(6, 2))
    comfort_level = Column(String(50))
    is_trend_up  = Column(Boolean)
    recorded_at  = Column(DateTime)
    loaded_at    = Column(DateTime, default=datetime.utcnow)


# -- Config from environment variables ------------------------------------------
API_KEY    = os.getenv("OPENWEATHER_API_KEY", "YOUR_API_KEY")
CITIES     = os.getenv("WEATHER_CITIES", "Phnom Penh,Tokyo,London,New York,Sydney,Paris")
RAW_PATH   = "/opt/airflow/data/weather_raw.json"
CLEAN_PATH = "/opt/airflow/data/weather_clean"
DB_CONN    = "postgresql+psycopg2://weather:weather@postgres/weather_db"

# -- Default task arguments ----------------------------------------------------
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ============================================================================
# TASK 1 - EXTRACT
# ============================================================================
def extract_weather(**kwargs):
    """Fetch weather data from OpenWeatherMap and save as JSON."""
    os.makedirs(os.path.dirname(RAW_PATH), exist_ok=True)

    # Check if API key is unconfigured
    is_placeholder_key = not API_KEY or API_KEY in ["YOUR_OPENWEATHER_API_KEY", "YOUR_API_KEY", ""]

    if is_placeholder_key:
        error_msg = "No valid OpenWeatherMap API key found. Please configure OPENWEATHER_API_KEY in .env."
        log.error(error_msg)
        raise ValueError(error_msg)

    city_list = [c.strip() for c in CITIES.split(",")]
    all_weather_data = []

    for city in city_list:
        # 1. Fetch Current Weather
        weather_url = (
            "https://api.openweathermap.org/data/2.5/weather"
            f"?q={city}&appid={API_KEY}&units=metric"
        )
        log.info(f"Fetching weather and AQI for city: {city}")

        try:
            w_res = requests.get(weather_url, timeout=10)
            w_res.raise_for_status()
            weather_data = w_res.json()
            
            # 2. Fetch Air Quality (requires lat/lon from weather data)
            lat = weather_data.get("coord", {}).get("lat")
            lon = weather_data.get("coord", {}).get("lon")
            
            if lat and lon:
                aqi_url = (
                    "https://api.openweathermap.org/data/2.5/air_pollution"
                    f"?lat={lat}&lon={lon}&appid={API_KEY}"
                )
                a_res = requests.get(aqi_url, timeout=10)
                a_res.raise_for_status()
                aqi_data = a_res.json()
                
                # Merge AQI into weather object for Spark to process
                weather_data["air_quality"] = aqi_data.get("list", [{}])[0]
            
            all_weather_data.append(weather_data)
            
            log.info(f"  City     : {weather_data.get('name')}")
            log.info(f"  Temp     : {weather_data['main']['temp']} deg C")
            log.info(f"  AQI (1-5): {weather_data.get('air_quality', {}).get('main', {}).get('aqi')}")
        except Exception as e:
            log.error(f"Failed to fetch data for {city}: {e}")
            continue

    if not all_weather_data:
        raise ValueError("Failed to fetch weather data for all cities")

    with open(RAW_PATH, "w") as f:
        json.dump(all_weather_data, f, indent=2)

    log.info(f"Raw data saved to {RAW_PATH} with {len(all_weather_data)} records")
    
    if "ti" in kwargs:
        kwargs["ti"].xcom_push(key="raw_path", value=RAW_PATH)
    return RAW_PATH



# ============================================================================
# TASK 3 - LOAD  (Task 2 is the SparkSubmitOperator defined in the DAG below)
# ============================================================================
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

    # 3. Perform high-performance bulk insert with Trend Detection
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        log.info("Processing records with Trend Detection...")
        final_records = []
        
        for rec in records:
            # Check last record for this city to detect trends
            last_record = session.query(WeatherData)\
                .filter(WeatherData.city == rec['city'])\
                .order_by(desc(WeatherData.recorded_at))\
                .first()
            
            if last_record and rec.get('temp_c') and last_record.temp_c:
                # If current temp > last temp, trend is up
                rec['is_trend_up'] = float(rec['temp_c']) > float(last_record.temp_c)
            else:
                rec['is_trend_up'] = False
            
            final_records.append(rec)

        log.info("Performing bulk insert via SQLAlchemy Session...")
        session.bulk_insert_mappings(WeatherData, final_records)
        session.commit()
        log.info(f"Successfully loaded {len(final_records)} row(s) into weather_data via SQLAlchemy")
    except Exception as e:
        session.rollback()
        log.error(f"Failed to load rows into PostgreSQL: {e}")
        raise
    finally:
        session.close()
        engine.dispose()

    return len(records)


# ============================================================================
# TASK 4 - GENERATE INSIGHTS
# ============================================================================
def generate_insights(**kwargs):
    """Query the DB for the latest run and print a 'Unique' executive summary."""
    hook = PostgresHook(postgres_conn_id="postgres_weather")
    engine = hook.get_sqlalchemy_engine()
    
    query = """
        SELECT city, temp_c, aqi, comfort_level, is_trend_up 
        FROM weather_data 
        WHERE recorded_at >= NOW() - INTERVAL '1 hour'
        ORDER BY temp_c DESC
    """
    df = pd.read_sql(query, engine)
    
    if df.empty:
        log.warning("No fresh data found for insights.")
        return

    print("\n" + "="*60)
    print(" ENVIRONMENTAL INSIGHTS REPORT")
    print("="*60)
    
    hottest = df.iloc[0]
    coldest = df.iloc[-1]
    best_air = df.sort_values('aqi').iloc[0]
    
    print(f"Hottest City     : {hottest['city'].title()} ({hottest['temp_c']} deg C)")
    print(f"Coldest City     : {coldest['city'].title()} ({coldest['temp_c']} deg C)")
    print(f"Best Air Quality : {best_air['city'].title()} (AQI: {best_air['aqi']})")
    
    # Trends
    ups = df[df['is_trend_up'] == True]['city'].tolist()
    if ups:
        print(f"Warming Up       : {', '.join([c.title() for c in ups])}")
    
    # Comfort Summary
    comfort_counts = df['comfort_level'].value_counts()
    print("-" * 30)
    print("Comfort Distribution:")
    for status, count in comfort_counts.items():
        print(f" - {status.title()}: {count} cities")
    
    print("="*60 + "\n")


# ============================================================================
# DAG DEFINITION
# ============================================================================
with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    description="Daily weather ETL: API -> Spark -> PostgreSQL",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",   # daily at 06:00 UTC
    catchup=False,
    tags=["weather", "etl", "spark", "postgres"],
) as dag:

    # -- Task 1: Extract ------------------------------------------------------
    extract_task = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
        provide_context=True,
    )

    # -- Task 2: Transform (Spark) --------------------------------------------
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

    # -- Task 3: Load --------------------------------------------------------
    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        provide_context=True,
    )

    # -- Task 4: Insights ----------------------------------------------------
    insights_task = PythonOperator(
        task_id="generate_insights",
        python_callable=generate_insights,
        provide_context=True,
    )

    # -- Dependencies: extract -> transform -> load -> insights ------------------
    extract_task >> transform_task >> load_task >> insights_task
