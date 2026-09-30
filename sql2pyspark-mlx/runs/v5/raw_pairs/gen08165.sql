WITH filtered AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        pressure * humidity AS pressure_times_humidity
    FROM device_readings
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 09:00:00')
),
valid AS (
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        firmware,
        pressure_times_humidity,
        SUBSTRING(firmware, 1, 2) AS firmware_prefix,
        TRIM(firmware) AS clean_firmware
    FROM filtered
    WHERE reading_ts >= TO_DATE('2025-09-01')
),
scored AS (
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        clean_firmware,
        COUNT(pressure_times_humidity) OVER (PARTITION BY device_code ORDER BY pressure_times_humidity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_pressure_times_humidity,
        MAX(pressure_times_humidity) OVER (PARTITION BY device_code ORDER BY pressure_times_humidity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_pressure_times_humidity,
        MIN(pressure_times_humidity) OVER (PARTITION BY device_code ORDER BY pressure_times_humidity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_pressure_times_humidity,
        AVG(pressure_times_humidity) OVER (PARTITION BY device_code ORDER BY pressure_times_humidity, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS pressure_times_humidity_avg_2_prior
    FROM valid
),
prev_events AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM scored
),
flagged AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
daily AS (
    SELECT
        CASE WHEN last4_min_pressure_times_humidity >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END AS last4_min_pressure_times_humidity_tier,
        DATE_TRUNC('HOUR', reading_ts) AS hour_window,
        COUNT_IF(device_code = 'DV-04') AS dv_04_events,
        COUNT(*) FILTER (WHERE device_code = 'DV-02') AS dv_02_rows,
        PERCENTILE_APPROX(pressure_times_humidity_avg_2_prior, ARRAY(0.25, 0.5, 0.75)) AS pressure_times_humidity_avg_2_prior_quartiles,
        COUNT_IF(device_code = 'DV-02') AS dv_02_events,
        MIN(prev_reading_ts) AS first_prev_reading_ts
    FROM sessionized
    GROUP BY GROUPING SETS ((CASE WHEN last4_min_pressure_times_humidity >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('HOUR', reading_ts)), (CASE WHEN last4_min_pressure_times_humidity >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
    HAVING COUNT(*) FILTER (WHERE device_code = 'DV-02') < 1000
)
SELECT
    last4_min_pressure_times_humidity_tier,
    hour_window,
    dv_04_events,
    dv_02_rows,
    pressure_times_humidity_avg_2_prior_quartiles,
    dv_02_events,
    first_prev_reading_ts
FROM daily
ORDER BY first_prev_reading_ts DESC;
