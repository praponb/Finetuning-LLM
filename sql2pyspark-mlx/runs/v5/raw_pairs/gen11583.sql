WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 1 HOUR THEN 1 ELSE 0 END AS new_group_flag
    FROM device_readings
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM islands
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
islands_device_readings AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        new_group_flag,
        session_no,
        SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_2
    FROM with_gaps
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(temp_c) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY new_group_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_temp_c,
        LAG(humidity, 2) OVER (PARTITION BY device_code ORDER BY new_group_flag, id) AS prev_humidity
    FROM islands_device_readings
    WHERE device_code = 'DV-03'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TO_DATE(reading_ts) AS reading_ts_date
    FROM metrics
    WHERE reading_ts > DATE '2025-09-01'
)
SELECT
    id,
    reading_ts,
    humidity,
    status_bits,
    new_group_flag,
    session_no,
    session_no_2,
    first_known_temp_c,
    reading_ts_date
FROM cleaned
ORDER BY reading_ts DESC NULLS LAST;
