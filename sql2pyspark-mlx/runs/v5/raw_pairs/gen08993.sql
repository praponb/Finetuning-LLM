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
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
)
SELECT
    CASE WHEN status_code >= 465 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
    SUM(CASE WHEN host = 'h1' THEN latency_ms ELSE 0 END) AS h1_latency_ms,
    SIZE(COLLECT_SET(host)) AS num_hosts
FROM exploded
GROUP BY CASE WHEN status_code >= 465 THEN 'PREMIUM' ELSE 'STANDARD' END
