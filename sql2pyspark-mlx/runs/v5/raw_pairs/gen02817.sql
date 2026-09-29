WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, v
)
SELECT
    id,
    service,
    log_ts,
    key,
    SUM(status_code) OVER (PARTITION BY level ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code,
    AVG(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_avg_1h,
    SUM(latency_ms) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_sum_15m
FROM exploded;
