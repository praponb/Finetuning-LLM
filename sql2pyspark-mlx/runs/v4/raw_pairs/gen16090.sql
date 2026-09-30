WITH exploded AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, v
)
SELECT
    CASE WHEN latency_ms >= 2446 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_tier,
    k,
    level,
    SIZE(COLLECT_SET(k)) AS num_ks,
    ROUND(SUM(latency_ms) / NULLIF(COUNT(CASE WHEN host = 'h3' THEN 1 END), 0), 2) AS latency_ms_per_h3,
    SUM(CASE WHEN level = 'DEBUG' THEN latency_ms ELSE 0 END) AS debug_latency_ms,
    GROUPING(CASE WHEN latency_ms >= 2446 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
FROM exploded
GROUP BY GROUPING SETS ((CASE WHEN latency_ms >= 2446 THEN 'PREMIUM' ELSE 'STANDARD' END, k, level), (CASE WHEN latency_ms >= 2446 THEN 'PREMIUM' ELSE 'STANDARD' END, k), ())
ORDER BY k
