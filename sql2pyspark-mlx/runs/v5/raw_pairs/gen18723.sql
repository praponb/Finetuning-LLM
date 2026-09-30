WITH enriched AS (
    SELECT
        *,
        SUM(humidity) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_humidity,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_humidity,
        MAX(humidity) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_humidity
    FROM device_readings
),
latest AS (
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) = 1
),
base AS (
    SELECT
        *,
        COALESCE(last7_sum_humidity, 0) AS last7_sum_humidity_filled,
        COALESCE(last7_max_humidity, 0) AS last7_max_humidity_filled
    FROM latest
    WHERE status_bits BETWEEN 5 AND 9
),
with_gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessions_device_readings AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
base_device_readings AS (
    SELECT *, CASE WHEN last7_sum_humidity_filled >= 84 THEN 'High' WHEN last7_sum_humidity_filled >= 23 THEN 'Medium' ELSE 'Low' END AS last7_sum_humidity_filled_band
    FROM sessions_device_readings
    WHERE reading_ts >= DATE '2025-09-01'
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits,
    firmware,
    last7_avg_humidity
FROM base_device_readings
ORDER BY device_code DESC, id DESC
LIMIT 5;
