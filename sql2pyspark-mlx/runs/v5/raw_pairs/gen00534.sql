WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, value
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM flattened
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(status_code) OVER (PARTITION BY service, host ORDER BY burst_id, id, attr_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_status_code,
        MIN(status_code) OVER (PARTITION BY service, host ORDER BY burst_id, id, attr_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_status_code,
        SUM(status_code) OVER (PARTITION BY service, host ORDER BY burst_id, id, attr_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_status_code,
        ROW_NUMBER() OVER (PARTITION BY service ORDER BY new_group_flag, id, attr_key) AS seq_no
    FROM sessionized
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, attr_key) AS prev_log_ts
    FROM with_windows
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY host ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Grouped metrics
SELECT
    attr_key,
    session_no,
    COUNT(*) FILTER (WHERE service = 'billing') AS billing_rows,
    COUNT(*) FILTER (WHERE attr_key = 'version') AS version_rows,
    COUNT(DISTINCT service) AS distinct_services,
    ROUND(SUM(new_group_flag) / NULLIF(COUNT(CASE WHEN host = 'h2' THEN 1 END), 0), 2) AS new_group_flag_per_h2,
    APPROX_PERCENTILE(status_code, 0.9) AS status_code_p90,
    DENSE_RANK() OVER (PARTITION BY attr_key ORDER BY COUNT(*) FILTER (WHERE attr_key = 'version') DESC) AS version_rows_dense_rank
FROM sessions_app_logs
GROUP BY attr_key, session_no
ORDER BY session_no NULLS FIRST
