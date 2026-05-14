# ── Stage 1: uv installer ────────────────────────────────────────────────────
FROM ghcr.io/astral-sh/uv:0.4.10 AS uv

# ── Stage 2: final Airflow image ─────────────────────────────────────────────
FROM apache/airflow:2.9.1-python3.11

USER root

# Install Java (required by SparkSubmitOperator to launch spark-submit)
RUN apt-get update -o Acquire::Check-Valid-Until=false -o Acquire::Check-Date=false && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy uv binary from the installer stage
COPY --from=uv /uv /usr/local/bin/uv

USER airflow

# Copy project definition
COPY pyproject.toml /opt/airflow/pyproject.toml

# Install dependencies from pyproject.toml using uv
RUN uv pip install --system --no-cache -r /opt/airflow/pyproject.toml --extra spark


