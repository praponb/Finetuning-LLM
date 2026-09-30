WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        level,
        latency_ms,
        context,
        context['version'] AS version,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        context['trace'] AS trace,
        context['region'] AS region,
        CAST((status_code & 4) != 0 AS INT) AS has_fault
    FROM app_logs
    WHERE latency_ms = 1715
)
-- Multi-level aggregation
SELECT
    service,
    level,
    CASE WHEN has_fault >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS has_fault_tier,
    COUNT(*) FILTER (WHERE level = 'INFO') AS info_rows,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(latency_ms, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_quartiles,
    GROUPING(service) AS grouping_level
FROM unpacked
GROUP BY ROLLUP (service, level, CASE WHEN has_fault >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY has_fault_tier DESC
