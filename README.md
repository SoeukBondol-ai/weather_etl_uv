# Production-Grade Weather ETL Pipeline
### Modern Daily ETL Pipeline: OpenWeatherMap API -> Apache Airflow -> Local PySpark -> PostgreSQL (SQLAlchemy ORM)

<p align="center">
  <img src="https://img.shields.io/badge/Orchestrator-Apache%20Airflow%202.9.1-017CEB?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Apache Airflow" />
  <img src="https://img.shields.io/badge/Engine-Apache%20Spark%203.5.1-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark" />
  <img src="https://img.shields.io/badge/Database-PostgreSQL%2015-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL" />
  <img src="https://img.shields.io/badge/ORM-SQLAlchemy%201.4-D71F00?style=for-the-badge&logo=python&logoColor=white" alt="SQLAlchemy" />
  <img src="https://img.shields.io/badge/Package%20Manager-uv%200.4-DE5D83?style=for-the-badge&logo=rust&logoColor=white" alt="uv Package Manager" />
</p>

---

## Project Overview

This is a modern, production-ready, fully-dockerized daily Weather ETL (Extract, Transform, Load) data pipeline. It demonstrates industry-standard practices for orchestration, large-scale processing, object-relational mapping, and lightning-fast virtual environment management.

```text
            [ OpenWeather API ]
                     │
                     │ Request to get API
                     ▼
        [ Extract Python Operator ]
                     │
                     │ weather_raw.json
                     ▼
           [ Shared Volume Data ]
                     │
                     │ Read raw data JSON
                     ▼
          [ SparkSubmitOperator ]
                     │
                     │ Write clean data Parquet
                     ▼
         [ weather_clean/*.parquet ]
                     │
                     │ Reads Parquet
                     ▼
           [ Load to SQLAlchemy ]
                     │
                     │ Bulk Insert
                     ▼
        [ PostgreSQL (weather_db) ]
```


### Key Architectural Highlights
- **Apache Airflow Orchestration:** Automatically schedules and monitors daily jobs, with explicit failure retry limits.
- **Portability-First PySpark:** Uses PySpark in `local[*]` mode inside isolated Docker containers. This ensures deterministic execution and eliminates host Java/JVM dependency issues.
- **SQLAlchemy ORM Model Mapping:** Fully declared Postgres schemas with strict typing mapped through SQLAlchemy objects to deliver structured, transactional bulk-inserts.
- **Astral uv Package Management:** Drastically speeds up package resolution and image build cycles, backed by a declarative `pyproject.toml` and deterministic `uv.lock`.
- **Developer-Friendly Team Setup:** Heavy PySpark binaries (~400MB) are decoupled from default dependencies. Team members on Windows and macOS can sync lightweight environments instantly, while Docker containers automatically include PySpark.

---

## Project Structure

```text
weather_etl_uv/
├── docker-compose.yml         # Defines Postgres, pgAdmin, Webserver, Scheduler, Spark
├── Dockerfile                 # Extends Airflow image with Java, uv installer, and Spark extras
├── pyproject.toml             # Declarative source of truth for Python dependencies
├── uv.lock                    # Deterministic, pinned package lockfile
├── .dockerignore              # Drastically reduces build context for rapid builds
├── .env                       # API keys and container configuration profiles
│
├── dags/
│   └── weather_etl.py         # Airflow DAG containing model definitions and tasks
├── spark/
│   └── transform.py           # Portable PySpark cleaning and feature-engineering script
├── sql/
│   ├── init.sql               # Automatically triggers on Postgres boot to map databases and grants
│   └── queries.sql            # Analytical queries for exploring database entries
├── logs/                      # Mounted folder for Airflow task execution logs
├── data/                      # Shared storage volume for raw JSON and intermediate Parquet data
└── plugins/                   # Folder for extending Airflow capabilities
```

---

## Quick Start (Run in under 2 minutes)

### Step 1 - Prerequisites
Ensure you have the following installed on your machine:
- **Docker Desktop** (with Docker Compose)
- **OpenWeatherMap API Key** (Get a free key at openweathermap.org/api)

---

### Step 2 - Edit Configuration
Open `.env` and configure your API key and target city:
```ini
# OpenWeatherMap API Configuration
OPENWEATHER_API_KEY=your_openweathermap_api_key_here
WEATHER_CITY=Phnom Penh
```

---

### Step 3 - Fire Up the Infrastructure
Start your entire cluster with a single command:
```bash
docker compose up --build -d
```

Verify that all systems are running and healthy:
```bash
docker compose ps
```

---

## Web UI Access & Monitoring

You can manage, trigger, and inspect the entire pipeline directly from your web browser.

### 1. Airflow Orchestration Web UI
- **URL:** http://localhost:8080
- **Credentials:** Username: `admin` / Password: `admin`
- **Actions:** Click on `weather_etl_pipeline` to inspect the Graph/Grid views, review task logs, or click the **Play** button in the top right to trigger a new DAG run manually.

### 2. pgAdmin Database Management Web UI
- **URL:** http://localhost:8081
- **Credentials:** Email: `admin@admin.com` / Password: `admin`
- **Setup Server Connection (Saved Permanently):**
  1. Click **Add New Server**.
  2. **General Tab:** Name it `Weather ETL DB`.
  3. **Connection Tab:** Enter Hostname: `postgres`, Port: `5432`, Maintenance DB: `weather_db`, Username: `weather`, Password: `weather`.
  4. Click **Save**. (Your settings are persisted in a Docker volume).
- **View Data:** Navigate to `Weather ETL DB` -> `Databases` -> `weather_db` -> `Schemas` -> `public` -> `Tables` -> `weather_data`. Right-click `weather_data` -> `View/Edit Data` -> `All Rows`.

### 3. Spark Cluster UI
- **URL:** http://localhost:8090
- **Actions:** Monitor Spark Master and Worker resource allocation during transformation jobs.

---

## Production Configurations & Services Summary

| Service | Access URL | Port | Credentials | Purpose |
|---|---|---|---|---|
| **Airflow Webserver** | http://localhost:8080 | `8080` | `admin` / `admin` | Dag orchestration UI & Monitoring |
| **pgAdmin Web UI** | http://localhost:8081 | `8081` | `admin@admin.com` / `admin` | Database administration UI |
| **PostgreSQL DB** | `localhost:5432` | `5432` | `weather` / `weather` | Target Data Warehouse (`weather_db`) |
| **Spark Master UI** | http://localhost:8090 | `8090` | *None* | Spark cluster performance monitoring |

---

## Local Development & Team Collaboration

This repository is optimized for cross-platform team development across Linux, Windows, and macOS.

### Lightweight Environment Sync
To ensure lightning-fast setup times on developer machines, heavy dependencies like PySpark (~400MB) are excluded from default syncs.

```bash
# 1. Install lightweight core dependencies instantly (Airflow, Pandas, SQLAlchemy)
uv sync

# 2. Opt-in to install PySpark locally when modifying Spark transform scripts
uv sync --extra spark

# 3. Install development tools (Ruff, Pytest)
uv sync --extra dev

# 4. Run code quality checks
uv run ruff check .
```

---

## Tear Down & Reset

To stop services while keeping your database entries and pgAdmin configurations safe:
```bash
docker compose down
```

To run a complete clean reset and erase all volumes and tables:
```bash
docker compose down -v
```
