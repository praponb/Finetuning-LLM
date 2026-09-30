WITH flattened AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
)
SELECT
    key,
    CASE WHEN latency_ms >= 1890 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_tier,
    COUNT(*) FILTER (WHERE host = 'h2') AS h2_rows,
    SUM(CASE WHEN service = 'auth' THEN latency_ms ELSE 0 END) AS auth_latency_ms
FROM flattened
GROUP BY key, CASE WHEN latency_ms >= 1890 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY auth_latency_ms
