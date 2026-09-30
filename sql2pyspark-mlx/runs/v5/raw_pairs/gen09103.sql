WITH flattened AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, v
)
SELECT
    CASE WHEN status_code >= 399 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_tier,
    level,
    COUNT(*) FILTER (WHERE attr_key = 'trace') AS trace_rows,
    COUNT(*) AS event_count,
    BIT_OR(latency_ms) AS latency_ms_bit_or,
    STDDEV_SAMP(latency_ms) AS latency_ms_stddev,
    COUNT(CASE WHEN host = 'h4' THEN 1 END) AS h4_count
FROM flattened
GROUP BY CASE WHEN status_code >= 399 THEN 'PREMIUM' ELSE 'STANDARD' END, level
HAVING STDDEV_SAMP(latency_ms) > 0;
