WITH lines AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, v
)
SELECT
    service,
    CASE WHEN latency_ms >= 2163 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_tier,
    APPROX_PERCENTILE(latency_ms, 0.9) AS latency_ms_p90,
    SUM(CASE WHEN service = 'billing' THEN latency_ms ELSE 0 END) AS billing_latency_ms,
    STDDEV_SAMP(latency_ms) AS latency_ms_stddev,
    AVG(latency_ms) AS avg_latency_ms
FROM lines
GROUP BY service, CASE WHEN latency_ms >= 2163 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING avg_latency_ms > 0
ORDER BY billing_latency_ms DESC;
