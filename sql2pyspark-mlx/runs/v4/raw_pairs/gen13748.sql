WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 1 HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(device_code) AS clean_device_code,
        TRIM(firmware) AS clean_firmware
    FROM islands
    WHERE reading_ts < '2024-11-01'
),
filtered_device_readings AS (
    -- Keep recent, valid rows
    SELECT *, humidity * session_no AS humidity_times_session_no
    FROM filtered
    WHERE reading_ts > DATE '2025-09-01'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS session_start_flag_2
    FROM filtered_device_readings
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag_2) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM sessionized
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity_times_session_no, id) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY session_id DESC) AS session_id_dense_rank,
        SUM(status_bits) OVER (PARTITION BY device_code ORDER BY session_start_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_bits,
        COUNT(session_start_flag_2) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS session_start_flag_2_count_30m
    FROM latest
),
daily AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('DAY', reading_ts) AS activity_day,
        CASE WHEN humidity >= 63 THEN 'LARGE' ELSE 'SMALL' END AS humidity_tier,
        MAX(session_no) AS max_session_no,
        PERCENTILE_APPROX(session_id, ARRAY(0.25, 0.5, 0.75)) AS session_id_quartiles,
        MAX(reading_ts) AS ended_at
    FROM with_windows
    GROUP BY DATE_TRUNC('DAY', reading_ts), CASE WHEN humidity >= 63 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    activity_day,
    humidity_tier,
    max_session_no,
    session_id_quartiles
FROM daily
ORDER BY max_session_no NULLS FIRST;
