WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        firmware,
        CASE WHEN humidity >= 65 THEN 'High' WHEN humidity >= 47 THEN 'Medium' ELSE 'Low' END AS humidity_level,
        CONCAT(firmware, ', ', device_code) AS firmware_device_code
    FROM device_readings
    WHERE device_code IN ('DV-01', 'DV-03', 'DV-02')
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM base
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN prev_reading_ts - LAG(prev_reading_ts) OVER (PARTITION BY device_code ORDER BY prev_reading_ts, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions_device_readings
),
sessions_device_readings_2 AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        humidity,
        prev_reading_ts,
        new_group_flag,
        session_no,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY prev_reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps_2
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY session_no DESC) AS session_no_cume_dist,
        ROW_NUMBER() OVER w_running AS seq_no,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM sessions_device_readings_2
    WINDOW w_running AS (ORDER BY burst_id, id), w_grp AS (PARTITION BY device_code ORDER BY session_no DESC, id)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        humidity,
        prev_reading_ts,
        session_no_cume_dist,
        seq_no,
        row_seq,
        MIN(seq_no) OVER (PARTITION BY device_code) AS device_code_min_seq_no,
        SUM(seq_no) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no,
        ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) AS humidity_share
    FROM metrics
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(running_seq_no, 0) AS running_seq_no_filled
    FROM scored
    WHERE humidity_share BETWEEN 0.58 AND 0.67
),
metrics_device_readings AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(row_seq) OVER w_grp AS running_sum_row_seq,
        MAX(row_seq) OVER w_grp AS running_max_row_seq,
        MIN(row_seq) OVER w_grp AS running_min_row_seq
    FROM filtered
    WINDOW w_grp AS (ORDER BY prev_reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    humidity,
    row_seq,
    device_code_min_seq_no,
    running_seq_no,
    humidity_share,
    running_seq_no_filled,
    running_sum_row_seq,
    running_max_row_seq
FROM metrics_device_readings
ORDER BY running_seq_no_filled DESC;
