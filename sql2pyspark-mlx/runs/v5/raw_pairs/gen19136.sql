WITH gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) > INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    id,
    device_code,
    reading_ts,
    humidity,
    pressure,
    firmware,
    session_start_flag,
    CASE WHEN device_code RLIKE '^(DV-02|DV-03)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS device_code_zone
FROM islands
ORDER BY pressure DESC NULLS LAST;
