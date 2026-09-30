WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, v
)
-- Window metrics over several different partitions
SELECT
    id,
    key,
    SUM(latency_ms) OVER (PARTITION BY key ORDER BY latency_ms, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_latency_ms,
    MIN(latency_ms) OVER (PARTITION BY key ORDER BY latency_ms, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_latency_ms,
    MAX(latency_ms) OVER (PARTITION BY key ORDER BY latency_ms, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_latency_ms,
    COUNT(latency_ms) OVER (PARTITION BY key ORDER BY latency_ms, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_latency_ms,
    LAG(latency_ms, 1, 0) OVER (PARTITION BY key ORDER BY latency_ms, id, key) AS prev_latency_ms
FROM flattened
WHERE key = 'version'
