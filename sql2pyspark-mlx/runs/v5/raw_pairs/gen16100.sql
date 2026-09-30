WITH cleaned AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        firmware,
        CASE WHEN temp_c >= 37 THEN 'High' WHEN temp_c >= 23.7 THEN 'Medium' ELSE 'Low' END AS temp_c_level
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND status_bits BETWEEN 4 AND 6
),
flagged AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 1 HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
metrics AS (
    SELECT
        id,
        device_code,
        reading_ts,
        firmware,
        temp_c_level,
        session_start_flag,
        CORR(pressure, humidity) OVER (PARTITION BY device_code) AS corr_pressure_humidity,
        AVG(burst_id) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS burst_id_avg_5_prior
    FROM islands
    WHERE burst_id > 8
),
with_gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM metrics
),
sessionized AS (
    SELECT
        id,
        device_code,
        reading_ts,
        firmware,
        temp_c_level,
        session_start_flag,
        corr_pressure_humidity,
        burst_id_avg_5_prior,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS rank_in_group
        FROM sessionized
    ) ranked
    WHERE rank_in_group = 1
),
with_gaps_2 AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM latest
),
sessionized_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        firmware,
        temp_c_level,
        session_start_flag,
        corr_pressure_humidity,
        burst_id_avg_5_prior,
        session_no,
        rank_in_group,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps_2
),
stats AS (
    SELECT
        burst_id,
        DATE_TRUNC('HOUR', reading_ts) AS hour_window,
        SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
        COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END) AS dv_04_count
    FROM sessionized_device_readings
    GROUP BY 1, 2
)
SELECT burst_id, hour_window
FROM stats;
