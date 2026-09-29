WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM device_readings
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    status_bits,
    firmware,
    prev_reading_ts,
    is_new_session,
    CASE WHEN device_code RLIKE '^(DV-01|DV-04)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS device_code_zone
FROM islands
ORDER BY is_new_session DESC NULLS FIRST
