WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM device_readings
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(prev_reading_ts) OVER (PARTITION BY device_code ORDER BY prev_reading_ts, id) AS prev_prev_reading_ts
    FROM islands
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_prev_reading_ts IS NULL OR prev_reading_ts > prev_prev_reading_ts + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY prev_reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        temp_c,
        humidity,
        pressure,
        prev_prev_reading_ts,
        session_start_flag,
        burst_id,
        LEAD(humidity) OVER w_ordered next_humidity,
        LEAD(new_group_flag) OVER w_ordered next_new_group_flag,
        LAG(new_group_flag) OVER w_ordered prev_new_group_flag,
        ROW_NUMBER() OVER w_ordered seq_no
    FROM sessions_device_readings
    WINDOW w_ordered AS (PARTITION BY device_code ORDER BY prev_prev_reading_ts, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        pressure,
        prev_prev_reading_ts,
        session_start_flag,
        next_humidity,
        seq_no,
        SUM(session_start_flag) OVER (ORDER BY session_start_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_session_start_flag,
        FIRST_VALUE(session_start_flag) IGNORE NULLS OVER (ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_session_start_flag
    FROM with_windows
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(pressure) OVER w_ordered next_pressure,
        LAG(pressure) OVER w_ordered prev_pressure,
        LEAD(first_known_session_start_flag) OVER w_ordered next_first_known_session_start_flag,
        LAG(first_known_session_start_flag) OVER w_ordered prev_first_known_session_start_flag,
        ROW_NUMBER() OVER w_ordered event_seq,
        SUM(seq_no) OVER (ORDER BY CAST(prev_prev_reading_ts AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) seq_no_sum_30d
    FROM metrics
    WHERE first_known_session_start_flag <> 0
    WINDOW w_ordered AS (ORDER BY seq_no, id)
),
with_windows_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(seq_no_sum_30d) OVER w_part running_count_seq_no_sum_30d,
        MAX(seq_no_sum_30d) OVER w_part running_max_seq_no_sum_30d,
        SUM(seq_no_sum_30d) OVER w_part running_sum_seq_no_sum_30d
    FROM scored
    WINDOW w_part AS (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', prev_prev_reading_ts) AS year_start,
    CASE WHEN seq_no_sum_30d >= 35 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_sum_30d_tier,
    DATE_TRUNC('MONTH', prev_prev_reading_ts) AS activity_month,
    MAX(prev_prev_reading_ts) AS last_prev_prev_reading_ts,
    PERCENTILE_APPROX(next_humidity, ARRAY(0.25, 0.5, 0.75)) AS next_humidity_quartiles
FROM with_windows_device_readings
GROUP BY DATE_TRUNC('YEAR', prev_prev_reading_ts), CASE WHEN seq_no_sum_30d >= 35 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', prev_prev_reading_ts)
ORDER BY seq_no_sum_30d_tier NULLS LAST
