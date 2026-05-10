-- ============================================================
-- queries.sql — useful queries to verify and explore data
-- Run these in pgAdmin, DBeaver, or psql
-- Connect: host=localhost port=5432 db=weather_db user=weather password=weather
-- ============================================================

-- 1. View all loaded records
SELECT * FROM weather_data ORDER BY loaded_at DESC LIMIT 20;

-- 2. Latest record per city
SELECT DISTINCT ON (city)
    city, temp_c, humidity, description, heat_index, recorded_at
FROM weather_data
ORDER BY city, recorded_at DESC;

-- 3. Daily average temperature
SELECT
    DATE(recorded_at)  AS date,
    city,
    ROUND(AVG(temp_c)::numeric, 1)  AS avg_temp,
    ROUND(AVG(humidity)::numeric, 0) AS avg_humidity,
    COUNT(*)            AS records
FROM weather_data
GROUP BY DATE(recorded_at), city
ORDER BY date DESC;

-- 4. Heat index distribution
SELECT heat_index, COUNT(*) AS count
FROM weather_data
GROUP BY heat_index
ORDER BY count DESC;

-- 5. How many rows have been loaded total
SELECT COUNT(*) AS total_rows FROM weather_data;

-- 6. Check for duplicates (same city + recorded_at)
SELECT city, recorded_at, COUNT(*) AS cnt
FROM weather_data
GROUP BY city, recorded_at
HAVING COUNT(*) > 1;
