WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, value
)
-- Multi-level aggregation
SELECT
    host,
    MAX(latency_ms) FILTER (WHERE service = 'search') AS search_max_latency_ms,
    SUM(CASE WHEN host = 'h3' THEN latency_ms ELSE 0 END) AS h3_latency_ms,
    SUM(CASE WHEN service = 'checkout' THEN latency_ms ELSE 0 END) AS checkout_latency_ms,
    COUNT(CASE WHEN attr_key = 'version' THEN 1 END) AS version_count,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list,
    COUNT(CASE WHEN attr_key = 'trace' THEN 1 END) AS trace_count
FROM lines
GROUP BY host
ORDER BY trace_count DESC;
