WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        log_ts,
        latency_ms,
        key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1
    FROM flattened
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(key, 'unassigned') AS key_filled
    FROM flattened_attrs
    WHERE log_ts > TO_TIMESTAMP('2025-06-01 08:00:00')
      AND key = 'version'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) AS prev_log_ts
    FROM base
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(prev_log_ts) OVER (PARTITION BY key ORDER BY prev_log_ts, id, key) IS NULL
                 OR prev_log_ts > LAG(prev_log_ts) OVER (PARTITION BY key ORDER BY prev_log_ts, id, key) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM islands
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY key ORDER BY prev_log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps_2
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        log_ts,
        latency_ms,
        key,
        status_code_high,
        flag_bit1,
        key_filled,
        prev_log_ts,
        is_new_burst,
        burst_id,
        is_new_session,
        session_no,
        NTILE(4) OVER (PARTITION BY key ORDER BY flag_bit1 DESC, id, key) AS flag_bit1_bucket,
        RANK() OVER w_running AS burst_id_rank,
        DENSE_RANK() OVER w_running AS burst_id_dense_rank,
        STDDEV_SAMP(status_code_high) OVER (PARTITION BY key) AS key_stddev_samp_status_code_high,
        LAG(burst_id, 1) OVER (PARTITION BY key ORDER BY is_new_burst, id, key) AS prev_burst_id
    FROM sessions_app_logs
    WINDOW w_running AS (PARTITION BY key ORDER BY burst_id DESC)
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(prev_log_ts) OVER (PARTITION BY key ORDER BY prev_log_ts, id, key) AS prev_prev_log_ts
    FROM scored
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_prev_log_ts IS NULL OR prev_log_ts > prev_prev_log_ts + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessions_app_logs_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY key ORDER BY prev_log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_2
    FROM flagged
)
-- Ranking and gap features
SELECT
    *,
    NTILE(3) OVER w_ordered AS flag_bit1_bucket_2,
    SUM(flag_bit1_bucket) OVER (PARTITION BY key) AS key_sum_flag_bit1_bucket
FROM sessions_app_logs_2
WINDOW w_ordered AS (PARTITION BY key ORDER BY flag_bit1 DESC, id, key);
