WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM device_readings
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        status_bits,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    id,
    device_code,
    reading_ts,
    humidity,
    status_bits,
    burst_id,
    CASE WHEN device_code RLIKE '^(DV-03|DV-02)$' THEN 'OTHER_ZONE' WHEN device_code RLIKE '^(DV-01|DV-04)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS device_code_zone
FROM sessionized;
