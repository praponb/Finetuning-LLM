WITH flattened AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
),
latest AS (
    SELECT *
    FROM flattened
    QUALIFY ROW_NUMBER() OVER (PARTITION BY host, level ORDER BY latency_ms NULLS LAST, id, key) = 1
),
kept_keys AS (
    SELECT host, id
    FROM latest
    EXCEPT
    SELECT host, id
    FROM app_logs
    WHERE log_ts >= '2025-04-01'
),
verified AS (
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.host = k.host AND s.id = k.id
),
enriched AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        key,
        attr_value,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host), 4) AS latency_ms_share,
        MAX(latency_ms) OVER (PARTITION BY host, level) AS host_max_latency_ms
    FROM verified
),
metrics AS (
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY key, level), 4) AS latency_ms_share_2,
        AVG(host_max_latency_ms) OVER w_ordered AS host_avg_host_max_latency_ms
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY host)
),
metrics_app_logs AS (
    SELECT
        *,
        MAX(host_max_latency_ms) OVER (PARTITION BY key) AS key_max_host_max_latency_ms,
        DENSE_RANK() OVER (PARTITION BY key ORDER BY host_max_latency_ms DESC NULLS LAST) AS host_max_latency_ms_dense_rank
    FROM metrics
)
SELECT
    *,
    RANK() OVER (PARTITION BY host ORDER BY host_max_latency_ms DESC NULLS FIRST) AS host_max_latency_ms_rank,
    SUM(latency_ms_share_2) OVER (PARTITION BY host, key) AS host_sum_latency_ms_share_2
FROM metrics_app_logs;
