WITH parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        payload_json,
        context['region'] AS region,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        COALESCE(context['version'], 'unknown') AS version_or_default,
        SIZE(context) AS num_context
    FROM app_logs
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    SIZE(COLLECT_SET(host)) AS num_hosts,
    ROUND(SUM(latency_ms) / NULLIF(COUNT(CASE WHEN host = 'h3' THEN 1 END), 0), 2) AS latency_ms_per_h3,
    SUM(num_context) AS total_num_context,
    COUNT(CASE WHEN host = 'h3' THEN 1 END) AS h3_count
FROM parsed
GROUP BY DATE_TRUNC('HOUR', log_ts)
ORDER BY h3_count DESC;
