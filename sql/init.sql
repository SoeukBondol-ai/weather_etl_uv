-- ============================================================
-- This file runs automatically when PostgreSQL container
-- starts for the first time (docker-entrypoint-initdb.d)
-- ============================================================

-- Create a separate database for weather data
-- (Airflow metadata uses the default 'airflow' database)
CREATE DATABASE weather_db;

-- Connect to weather_db and create user + table
\connect weather_db

CREATE USER weather WITH PASSWORD 'weather';
GRANT ALL PRIVILEGES ON DATABASE weather_db TO weather;
GRANT ALL ON SCHEMA public TO weather;

-- Weather data table
CREATE TABLE IF NOT EXISTS weather_data (
    id          SERIAL PRIMARY KEY,
    city        VARCHAR(100)   NOT NULL,
    temp_c      NUMERIC(5,1),
    feels_like  NUMERIC(5,1),
    humidity    INTEGER,
    pressure    INTEGER,
    wind_speed  NUMERIC(5,2),
    description TEXT,
    heat_index  VARCHAR(20),
    recorded_at TIMESTAMP,
    loaded_at   TIMESTAMP      DEFAULT NOW()
);

-- Index for common queries
CREATE INDEX idx_weather_city       ON weather_data(city);
CREATE INDEX idx_weather_recorded   ON weather_data(recorded_at);

-- Grant privileges on tables and sequences to weather user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO weather;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO weather;

-- Verify
\echo '  weather_db and weather_data table created successfully'
