WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, v
)
SELECT
    id,
    host,
    log_ts,
    level,
    latency_ms,
    key,
    v,
    SUM(latency_ms) OVER (PARTITION BY level ORDER BY latency_ms ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_latency_ms,
    NTILE(5) OVER (PARTITION BY service ORDER BY latency_ms ASC, id ASC, key ASC) AS latency_ms_bucket,
    LAST_VALUE(key, TRUE) OVER w_hist AS last_known_key,
    AVG(latency_ms) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_avg_15m,
    LEAD(latency_ms, 2) OVER (PARTITION BY host, key ORDER BY latency_ms ASC, id ASC, key ASC) AS next_latency_ms
FROM exploded
WINDOW w_hist AS (PARTITION BY key, host ORDER BY log_ts ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
