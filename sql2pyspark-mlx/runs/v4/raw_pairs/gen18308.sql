WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, attr_value
),
parsed AS (
    SELECT
        id,
        service,
        status_code,
        key,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS has_fault
    FROM flattened
    WHERE key <> 'region'
),
metrics AS (
    SELECT
        id,
        service,
        status_code,
        key,
        attr_value,
        status_code_high,
        has_fault,
        PERCENT_RANK() OVER (ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY service ORDER BY status_code_high DESC, id, key) AS seq_no,
        NTILE(3) OVER w_running AS status_code_high_bucket
    FROM parsed
    WINDOW w_running AS (PARTITION BY key ORDER BY status_code_high, id, key)
)
SELECT
    key,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    COUNT(*) FILTER (WHERE service = 'auth') AS auth_rows,
    APPROX_PERCENTILE(has_fault, 0.9) AS has_fault_p90,
    COUNT_IF(key = 'version') AS version_events
FROM metrics
GROUP BY key
ORDER BY version_events
