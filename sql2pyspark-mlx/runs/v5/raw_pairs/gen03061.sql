WITH lines AS (
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, v
)
SELECT
    key,
    COUNT(CASE WHEN level = 'ERROR' THEN 1 END) AS error_count,
    COUNT(CASE WHEN level = 'DEBUG' THEN 1 END) AS debug_count
FROM lines
GROUP BY key
