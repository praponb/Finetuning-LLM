WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM device_readings
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_bits >= 10 THEN 'HIGH' ELSE 'LOW' END AS status_bits_tier,
    CASE WHEN pressure >= 1018 THEN 'LARGE' ELSE 'SMALL' END AS pressure_tier,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    COUNT(DISTINCT firmware) AS distinct_firmwares
FROM sessions_device_readings
GROUP BY GROUPING SETS ((CASE WHEN status_bits >= 10 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN pressure >= 1018 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN status_bits >= 10 THEN 'HIGH' ELSE 'LOW' END), ())
ORDER BY status_bits_tier DESC;
