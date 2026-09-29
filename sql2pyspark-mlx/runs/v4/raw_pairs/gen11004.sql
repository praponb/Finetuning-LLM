WITH base AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        pressure * temp_c AS pressure_times_temp_c,
        DATEDIFF(CURRENT_DATE(), reading_ts) AS days_since_reading_ts
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 10:00:00'
),
prev_events AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM base
),
with_gaps AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    CASE WHEN pressure_times_temp_c >= 81 THEN 'PREMIUM' ELSE 'STANDARD' END AS pressure_times_temp_c_tier,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    APPROX_PERCENTILE(is_new_burst, 0.9) AS is_new_burst_p90
FROM sessionized
GROUP BY 1
ORDER BY pressure_times_temp_c_tier DESC
