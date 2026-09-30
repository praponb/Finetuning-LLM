WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, value
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_code >= 385 THEN 'LARGE' ELSE 'SMALL' END AS status_code_tier,
    STDDEV_SAMP(status_code) AS status_code_stddev,
    COUNT(*) FILTER (WHERE host = 'h2') AS h2_rows,
    ROUND(SUM(latency_ms) / NULLIF(COUNT(CASE WHEN level = 'INFO' THEN 1 END), 0), 2) AS latency_ms_per_info,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes
FROM exploded
GROUP BY CASE WHEN status_code >= 385 THEN 'LARGE' ELSE 'SMALL' END
HAVING status_code_stddev > 0
