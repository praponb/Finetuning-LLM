WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, v
)
SELECT
    k,
    CASE WHEN latency_ms >= 1824 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_tier,
    CASE WHEN status_code >= 414 THEN 'HIGH' ELSE 'LOW' END AS status_code_tier,
    APPROX_PERCENTILE(status_code, 0.9) AS status_code_p90,
    PERCENTILE_APPROX(status_code, 0.5) AS status_code_p50,
    SUM(CASE WHEN k = 'version' THEN status_code ELSE 0 END) AS version_status_code,
    PERCENTILE_APPROX(latency_ms, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_quartiles,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    MAX(log_ts) AS ended_at
FROM lines
GROUP BY 1, 2, 3
ORDER BY span_minutes DESC;
