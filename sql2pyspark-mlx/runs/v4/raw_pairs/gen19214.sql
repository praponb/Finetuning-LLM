WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, value
),
flattened_attrs AS (
    SELECT
        id,
        service,
        log_ts,
        status_code,
        attr_key,
        value,
        CAST((status_code & 4) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
with_windows AS (
    SELECT
        *,
        LEAD(status_code_high, 1) OVER (PARTITION BY service, attr_key ORDER BY log_ts, id, attr_key) AS next_status_code_high,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY service ORDER BY log_ts DESC, id, attr_key) AS event_seq
    FROM flattened_attrs
    WINDOW w_ordered AS (ORDER BY status_code_high, id, attr_key)
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM with_windows
),
sessions_app_logs AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
enriched AS (
    SELECT
        *,
        CUME_DIST() OVER w_grp AS status_code_high_cume_dist,
        AVG(session_start_flag) OVER w_hist AS session_start_flag_avg_6,
        ROW_NUMBER() OVER (ORDER BY status_code_high, id, attr_key) AS event_seq_2
    FROM sessions_app_logs
    WINDOW w_grp AS (PARTITION BY service, attr_key ORDER BY status_code_high DESC NULLS LAST), w_hist AS (PARTITION BY service ORDER BY session_start_flag, id, attr_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    service,
    status_code,
    attr_key,
    has_fault,
    status_code_high,
    session_start_flag,
    session_id,
    status_code_high_cume_dist
FROM enriched
ORDER BY service DESC NULLS LAST;
