WITH exploded AS (
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
    LATERAL VIEW EXPLODE(context) t AS key, attr_value
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_code >= 424 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    COUNT(*) FILTER (WHERE key = 'trace') AS trace_rows,
    SUM(CASE WHEN level = 'DEBUG' THEN latency_ms ELSE 0 END) AS debug_latency_ms,
    MAX(latency_ms) FILTER (WHERE level = 'WARN') AS warn_max_latency_ms,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes
FROM exploded
GROUP BY CASE WHEN status_code >= 424 THEN 'PREMIUM' ELSE 'STANDARD' END;
