WITH parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        payload_json,
        context,
        context['region'] AS region,
        COALESCE(context['region'], 'direct') AS region_or_default,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        ARRAY_CONTAINS(MAP_KEYS(context), 'trace') AS has_trace,
        context['trace'] AS trace
    FROM app_logs
)
-- Grouped metrics
SELECT
    CASE WHEN latency_ms >= 1809 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_tier,
    MAX(latency_ms) FILTER (WHERE service = 'billing') AS billing_max_latency_ms,
    COUNT(CASE WHEN service = 'billing' THEN 1 END) AS billing_count
FROM parsed
GROUP BY CASE WHEN latency_ms >= 1809 THEN 'LARGE' ELSE 'SMALL' END
HAVING billing_count < 1000
ORDER BY latency_ms_tier;
