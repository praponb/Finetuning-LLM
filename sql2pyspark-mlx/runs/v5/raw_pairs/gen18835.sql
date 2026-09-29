WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, v
),
flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        log_ts,
        status_code,
        k,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS has_fault
    FROM lines
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM flattened_attrs
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) AS prev_log_ts
    FROM sessionized
),
with_gaps_2 AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessionized_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps_2
)
-- Ranking and gap features
SELECT
    id,
    log_ts,
    k,
    has_fault,
    new_group_flag,
    session_no,
    prev_log_ts,
    is_new_session,
    SUM(new_group_flag) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_new_group_flag,
    COUNT(status_code_high) OVER (PARTITION BY k) AS k_count_status_code_high,
    LAG(status_code, 1) OVER (PARTITION BY k ORDER BY status_code_high, id, k) AS prev_status_code
FROM sessionized_app_logs
WHERE is_new_session >= 1;
