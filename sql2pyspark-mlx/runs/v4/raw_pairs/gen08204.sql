WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, value
)
-- Ranking and gap features
SELECT
    *,
    RANK() OVER (PARTITION BY key ORDER BY latency_ms DESC) AS latency_ms_rank,
    AVG(latency_ms) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_avg_1h,
    RANK() OVER (PARTITION BY key ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_rank_2
FROM flattened;
