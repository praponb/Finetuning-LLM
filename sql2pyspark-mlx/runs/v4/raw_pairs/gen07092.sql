WITH base AS (
    SELECT *, GREATEST(humidity, temp_c) AS greatest_humidity_temp_c
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND temp_c > 31
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM base
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
latest AS (
    SELECT *
    FROM sessionized
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) = 1
),
with_prev AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM latest
),
gaps AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessions_device_readings AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM gaps
),
latest_device_readings AS (
    SELECT *
    FROM sessions_device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY is_new_session DESC, id) = 1
),
latest_device_readings_2 AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure, id) AS rank_in_group
        FROM latest_device_readings
    ) t
    WHERE rank_in_group = 1
),
stats AS (
    SELECT
        burst_id,
        SUM(CASE WHEN device_code = 'DV-03' THEN is_new_burst ELSE 0 END) AS dv_03_is_new_burst,
        MIN(burst_id) AS min_burst_id,
        MAX(is_new_session) FILTER (WHERE device_code = 'DV-02') AS dv_02_max_is_new_session,
        MIN(reading_ts) AS first_seen,
        ROUND(AVG(pressure), 2) AS avg_pressure,
        SIZE(COLLECT_SET(device_code)) AS num_device_codes
    FROM latest_device_readings_2
    GROUP BY 1
)
SELECT
    burst_id,
    dv_03_is_new_burst,
    first_seen,
    avg_pressure,
    num_device_codes
FROM stats
ORDER BY num_device_codes DESC
