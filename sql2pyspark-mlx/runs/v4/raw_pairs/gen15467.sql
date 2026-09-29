WITH lines AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, attr_value
),
metrics AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        attr_value,
        DENSE_RANK() OVER (ORDER BY latency_ms DESC NULLS LAST) latency_ms_dense_rank,
        AVG(latency_ms) OVER (PARTITION BY service, level) service_avg_latency_ms,
        SUM(latency_ms) OVER (PARTITION BY level) level_sum_latency_ms
    FROM lines
),
with_windows AS (
    SELECT
        *,
        ROUND(level_sum_latency_ms / SUM(level_sum_latency_ms) OVER (PARTITION BY level), 4) level_sum_latency_ms_share,
        COUNT(service_avg_latency_ms) OVER (PARTITION BY service) service_count_service_avg_latency_ms,
        COUNT(level_sum_latency_ms) OVER (PARTITION BY service, k) service_count_level_sum_latency_ms
    FROM metrics
),
kept_keys AS (
    SELECT id, host
    FROM with_windows
    INTERSECT ALL
    SELECT id, host
    FROM app_logs
    WHERE latency_ms BETWEEN 1870 AND 2158
),
verified AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id AND s.host = k.host
),
summary AS (
    SELECT
        DATE_TRUNC('HOUR', log_ts) AS hour_window,
        ROUND(AVG(level_sum_latency_ms_share), 2) AS avg_level_sum_latency_ms_share,
        MAX(log_ts) AS ended_at,
        PERCENTILE_APPROX(service_avg_latency_ms, ARRAY(0.25, 0.5, 0.75)) AS service_avg_latency_ms_quartiles,
        ROUND(SUM(level_sum_latency_ms) / NULLIF(COUNT(CASE WHEN k = 'version' THEN 1 END), 0), 2) AS level_sum_latency_ms_per_version
    FROM verified
    GROUP BY DATE_TRUNC('HOUR', log_ts)
)
SELECT
    hour_window,
    service_avg_latency_ms_quartiles,
    level_sum_latency_ms_per_version
FROM summary
