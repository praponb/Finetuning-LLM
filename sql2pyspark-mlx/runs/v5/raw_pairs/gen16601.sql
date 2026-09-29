WITH exploded AS (
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
)
SELECT
    CASE WHEN latency_ms >= 2610 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_tier,
    PERCENTILE_APPROX(latency_ms, 0.95) AS latency_ms_p95,
    SUM(status_code) AS total_status_code,
    ROUND(SUM(latency_ms) / NULLIF(COUNT(CASE WHEN service = 'search' THEN 1 END), 0), 2) AS latency_ms_per_search,
    MAX(latency_ms) AS max_latency_ms
FROM exploded
GROUP BY CASE WHEN latency_ms >= 2610 THEN 'LARGE' ELSE 'SMALL' END
HAVING latency_ms_per_search > 0
ORDER BY latency_ms_tier NULLS LAST;
