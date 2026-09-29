WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
),
parsed AS (
    SELECT
        id,
        log_ts,
        status_code,
        key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS has_fault
    FROM exploded
),
filtered AS (
    SELECT *, TRIM(key) AS clean_key
    FROM parsed
    WHERE log_ts > DATE '2025-06-01'
      AND key IS NOT NULL
),
filtered_app_logs AS (
    SELECT *, COALESCE(key, 'unassigned') AS key_filled
    FROM filtered
    WHERE log_ts >= DATE '2025-06-01'
      AND key IS NOT NULL
)
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    key,
    COUNT(CASE WHEN key = 'region' THEN 1 END) AS region_count,
    SUM(CASE WHEN key = 'trace' THEN has_fault ELSE 0 END) AS trace_has_fault,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    MIN(log_ts) AS first_seen
FROM filtered_app_logs
GROUP BY DATE_TRUNC('DAY', log_ts), key;
