WITH ranked AS (
    SELECT *
    FROM device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) = 1
),
enriched AS (
    SELECT
        *,
        MIN(pressure) OVER w_part AS running_min_pressure,
        SUM(pressure) OVER w_part AS running_sum_pressure,
        COUNT(pressure) OVER w_part AS running_count_pressure,
        AVG(pressure) OVER w_part AS running_avg_pressure,
        NTILE(5) OVER (PARTITION BY device_code ORDER BY pressure DESC, id) AS pressure_bucket
    FROM ranked
    WINDOW w_part AS (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked_device_readings AS (
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) = 1
),
with_prev AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM ranked_device_readings
),
flagged AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
islands AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        status_bits,
        firmware,
        pressure_bucket,
        SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
enriched_device_readings AS (
    SELECT
        *,
        AVG(session_id) OVER w_part AS running_avg_session_id,
        COUNT(session_id) OVER w_part AS running_count_session_id,
        MAX(session_id) OVER w_part AS running_max_session_id,
        SUM(session_id) OVER w_part AS running_sum_session_id
    FROM islands
    WINDOW w_part AS (PARTITION BY device_code ORDER BY pressure_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    reading_ts,
    firmware,
    pressure_bucket,
    session_id,
    running_avg_session_id,
    running_count_session_id,
    running_max_session_id,
    running_sum_session_id
FROM enriched_device_readings
ORDER BY running_avg_session_id DESC
