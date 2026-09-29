WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, v
)
-- Aggregate with conditional metrics
SELECT
    key,
    MIN(log_ts) AS first_seen,
    COUNT(DISTINCT host) AS distinct_hosts
FROM flattened
GROUP BY key
HAVING distinct_hosts >= 1
ORDER BY distinct_hosts NULLS FIRST;
