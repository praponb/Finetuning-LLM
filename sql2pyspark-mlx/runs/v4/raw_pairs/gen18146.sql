WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, value
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(latency_ms) OVER w_running AS cumulative_latency_ms,
        MAX(latency_ms) OVER (PARTITION BY host) AS host_max_latency_ms,
        SUM(latency_ms) OVER (PARTITION BY k ORDER BY latency_ms, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_latency_ms,
        NTILE(5) OVER w_recent AS latency_ms_bucket
    FROM exploded
    WINDOW w_running AS (PARTITION BY host, service ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY latency_ms DESC, id, k)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        value,
        cumulative_latency_ms,
        host_max_latency_ms,
        running_latency_ms,
        latency_ms_bucket,
        AVG(host_max_latency_ms) OVER (ORDER BY host_max_latency_ms, id, k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS host_max_latency_ms_avg_2,
        FIRST_VALUE(cumulative_latency_ms, TRUE) OVER (ORDER BY host_max_latency_ms, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_latency_ms,
        SUM(running_latency_ms) OVER (ORDER BY host_max_latency_ms, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_latency_ms,
        AVG(latency_ms) OVER (PARTITION BY k ORDER BY running_latency_ms, id, k ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS latency_ms_avg_6,
        CUME_DIST() OVER (PARTITION BY host ORDER BY latency_ms_bucket DESC NULLS LAST) AS latency_ms_bucket_cume_dist
    FROM scored
)
SELECT
    id,
    level,
    latency_ms,
    k,
    value,
    cumulative_latency_ms,
    running_latency_ms,
    latency_ms_bucket,
    host_max_latency_ms_avg_2
FROM metrics
ORDER BY running_latency_ms DESC NULLS FIRST, latency_ms_bucket
