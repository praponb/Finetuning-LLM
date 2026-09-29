WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(humidity / pressure, 2) AS humidity_per_pressure
    FROM device_readings
    WHERE TO_DATE(reading_ts) < '2024-07-01'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c DESC, id) AS rn
        FROM valid
    ) x
    WHERE rn = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM ranked
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM sessions_device_readings
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Ranking and gap features
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits,
    firmware,
    humidity_per_pressure,
    rn,
    session_start_flag,
    island_id,
    is_new_session,
    session_id,
    FIRST_VALUE(humidity, TRUE) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_humidity,
    AVG(session_id) OVER (PARTITION BY device_code ORDER BY rn, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS session_id_avg_6_prior,
    LEAD(pressure, 1) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS next_pressure,
    ROUND(temp_c / SUM(temp_c) OVER (PARTITION BY device_code), 4) AS temp_c_share
FROM islands;
