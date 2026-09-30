WITH filtered AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        COALESCE(humidity, 0) AS humidity_filled,
        status_bits * pressure AS status_bits_times_pressure
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 06:00:00'
),
gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessions_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        humidity_filled,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    CASE WHEN temp_c >= 34.6 THEN 'PREMIUM' ELSE 'STANDARD' END AS temp_c_tier,
    CASE WHEN burst_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS burst_id_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(reading_ts) AS last_seen,
    APPROX_PERCENTILE(humidity_filled, ARRAY(0.25, 0.5, 0.75)) AS humidity_filled_quartiles
FROM sessions_device_readings
GROUP BY CASE WHEN temp_c >= 34.6 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN burst_id >= 8 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY distinct_ids DESC NULLS LAST
