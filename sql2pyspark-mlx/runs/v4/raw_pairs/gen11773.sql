WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    id,
    device_code,
    reading_ts,
    humidity,
    status_bits,
    session_start_flag,
    burst_id,
    CASE WHEN device_code RLIKE '^(DV-01|DV-03)$' THEN 'OTHER_ZONE' WHEN device_code RLIKE '^(DV-02|DV-04)$' THEN 'CORE' ELSE 'PRIORITY' END AS device_code_zone
FROM sessions_device_readings
ORDER BY session_start_flag DESC;
