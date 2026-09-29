WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CORR(status_code, latency_ms) OVER (PARTITION BY key, host) AS corr_status_code_latency_ms,
        AVG(latency_ms) OVER (PARTITION BY level) AS level_avg_latency_ms,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share,
        SUM(latency_ms) OVER (PARTITION BY service ORDER BY latency_ms, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_latency_ms
    FROM flattened
    WHERE log_ts < '2024-04-22'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY log_ts, id, key) AS dedup_rank
    FROM metrics
    QUALIFY dedup_rank = 1
),
metrics_app_logs AS (
    -- Running and moving aggregates
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        corr_status_code_latency_ms,
        latency_ms_share,
        running_latency_ms,
        dedup_rank,
        MAX(running_latency_ms) OVER w_part AS last4_max_running_latency_ms,
        SUM(running_latency_ms) OVER w_part AS last4_sum_running_latency_ms,
        COUNT(running_latency_ms) OVER w_part AS last4_count_running_latency_ms,
        AVG(running_latency_ms) OVER w_part AS last4_avg_running_latency_ms,
        SUM(level_avg_latency_ms) OVER (PARTITION BY level, service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS level_avg_latency_ms_sum_15m
    FROM deduped
    WINDOW w_part AS (PARTITION BY service ORDER BY status_code, id, key ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        latency_ms,
        status_code,
        key,
        latency_ms_share,
        running_latency_ms,
        last4_count_running_latency_ms,
        level_avg_latency_ms_sum_15m,
        COUNT(latency_ms_share) OVER (PARTITION BY key ORDER BY status_code, id, key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_latency_ms_share,
        MAX(latency_ms_share) OVER (PARTITION BY key ORDER BY status_code, id, key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_latency_ms_share,
        SUM(latency_ms_share) OVER (PARTITION BY key ORDER BY status_code, id, key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_latency_ms_share,
        MIN(latency_ms_share) OVER (PARTITION BY key ORDER BY status_code, id, key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_latency_ms_share,
        AVG(last4_count_running_latency_ms) OVER (ORDER BY last4_max_running_latency_ms, id, key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last4_count_running_latency_ms_avg_6
    FROM metrics_app_logs
)
-- Grouped metrics
SELECT
    CASE WHEN last7_min_latency_ms_share >= 0.89 THEN 'PREMIUM' ELSE 'STANDARD' END AS last7_min_latency_ms_share_tier,
    COUNT(CASE WHEN key = 'version' THEN 1 END) AS version_count,
    COUNT_IF(key = 'region') AS region_events
FROM scored
GROUP BY last7_min_latency_ms_share_tier
