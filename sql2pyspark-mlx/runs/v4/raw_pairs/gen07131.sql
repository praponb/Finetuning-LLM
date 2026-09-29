WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, TO_DATE(reading_ts) AS reading_ts_date
    FROM device_readings
    WHERE reading_ts >= '2024-09-01'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id ASC) = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id ASC) AS row_num
        FROM ranked
    ) x
    WHERE row_num = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM latest
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id ASC) AS latest_rank
    FROM islands
    QUALIFY latest_rank = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM deduped
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM sessions_device_readings
    EXCEPT ALL
    SELECT id
    FROM device_readings
    WHERE firmware <> '2.0.1'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_device_readings s
    JOIN remaining_keys k ON s.id = k.id
)
SELECT
    reading_ts,
    humidity,
    status_bits,
    reading_ts_date,
    row_num,
    is_new_session,
    latest_rank
FROM kept_rows
ORDER BY is_new_session DESC NULLS FIRST
