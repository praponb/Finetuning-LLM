WITH prev_events AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM device_readings
),
flagged AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessions_device_readings AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    CASE WHEN burst_id >= 8 THEN 'HIGH' ELSE 'LOW' END AS burst_id_tier,
    CASE WHEN temp_c >= 34.6 THEN 'PREMIUM' ELSE 'STANDARD' END AS temp_c_tier,
    device_code,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    CORR(humidity, pressure) AS corr_humidity_pressure,
    ROUND(SUM(pressure) / NULLIF(COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END), 0), 2) AS pressure_per_dv_04,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN burst_id >= 8 THEN 'HIGH' ELSE 'LOW' END ORDER BY ROUND(SUM(pressure) / NULLIF(COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END), 0), 2) DESC) AS pressure_per_dv_04_dense_rank
FROM sessions_device_readings
GROUP BY burst_id_tier, temp_c_tier, device_code
