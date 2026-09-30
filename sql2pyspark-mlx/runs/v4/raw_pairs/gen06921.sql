WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
)
-- Running and moving aggregates
SELECT
    id,
    service,
    log_ts,
    level,
    latency_ms,
    key,
    value,
    DENSE_RANK() OVER w_recent AS latency_ms_dense_rank,
    COUNT(latency_ms) OVER (PARTITION BY key) AS key_count_latency_ms,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level), 4) AS latency_ms_share,
    SUM(latency_ms) OVER (PARTITION BY service, key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_sum_2h,
    PERCENT_RANK() OVER (ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_percent_rank
FROM lines
WINDOW w_recent AS (ORDER BY latency_ms DESC NULLS FIRST);
