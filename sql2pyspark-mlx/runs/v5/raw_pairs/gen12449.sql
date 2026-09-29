WITH flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        context['trace'] AS trace,
        COALESCE(context['version'], 'none') AS version_or_default,
        SIZE(context) AS num_context,
        context['region'] AS region,
        ARRAY_CONTAINS(MAP_KEYS(context), 'region') AS has_region
    FROM app_logs
)
-- Grouped metrics
SELECT
    CASE WHEN num_context >= 73 THEN 'HIGH' ELSE 'LOW' END AS num_context_tier,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    SUM(latency_ms) FILTER (WHERE level = 'WARN') AS warn_total_latency_ms,
    COUNT(*) FILTER (WHERE host = 'h2') AS h2_rows,
    AVG(status_code) AS avg_status_code,
    ROUND(AVG(status_code), 2) AS avg_status_code_2,
    MAX(log_ts) AS last_seen,
    APPROX_PERCENTILE(latency_ms, 0.5) AS latency_ms_p50
FROM flattened_attrs
GROUP BY GROUPING SETS ((CASE WHEN num_context >= 73 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('HOUR', log_ts)), (CASE WHEN num_context >= 73 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING avg_status_code > 0
ORDER BY warn_total_latency_ms DESC;
