WITH flattened AS (
    -- Unnest the array into one row per element
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
    LATERAL VIEW OUTER EXPLODE(context) x AS key, v
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(latency_ms * 0.9, 2) AS latency_ms_adjusted,
        COALESCE(key, 'Unknown') AS key_filled,
        COALESCE(key, 'OTHER') AS resolved_key
    FROM flattened
    WHERE log_ts < '2024-08-01'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        v,
        latency_ms_adjusted,
        key_filled,
        resolved_key,
        AVG(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_avg_15m,
        STDDEV_SAMP(latency_ms_adjusted) OVER w_part AS service_stddev_samp_latency_ms_adjusted,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host), 4) AS latency_ms_share,
        STDDEV_SAMP(latency_ms_adjusted) OVER (PARTITION BY level, service) AS level_stddev_samp_latency_ms_adjusted,
        MAX(latency_ms) OVER w_part AS service_max_latency_ms
    FROM valid
    WINDOW w_part AS (PARTITION BY service)
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
        key,
        v,
        latency_ms_adjusted,
        key_filled,
        resolved_key,
        latency_ms_avg_15m,
        service_stddev_samp_latency_ms_adjusted,
        latency_ms_share,
        level_stddev_samp_latency_ms_adjusted,
        service_max_latency_ms,
        ROUND(service_max_latency_ms / SUM(service_max_latency_ms) OVER (PARTITION BY service), 4) AS service_max_latency_ms_share,
        DENSE_RANK() OVER (ORDER BY service_stddev_samp_latency_ms_adjusted DESC NULLS FIRST) AS service_stddev_samp_latency_ms_adjusted_dense_rank
    FROM scored
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        v,
        latency_ms_adjusted,
        key_filled,
        resolved_key,
        latency_ms_avg_15m,
        service_stddev_samp_latency_ms_adjusted,
        latency_ms_share,
        level_stddev_samp_latency_ms_adjusted,
        service_max_latency_ms_share,
        service_stddev_samp_latency_ms_adjusted_dense_rank,
        COALESCE(service_stddev_samp_latency_ms_adjusted_dense_rank, 0) AS service_stddev_samp_latency_ms_adjusted_dense_rank_filled,
        COALESCE(key, 'DIRECT') AS resolved_key_2
    FROM metrics
    WHERE log_ts > TIMESTAMP '2025-06-01 18:00:00'
      AND latency_ms BETWEEN 1262 AND 1811
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(service_max_latency_ms_share) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS service_max_latency_ms_share_count_15m,
        AVG(latency_ms_avg_15m) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_avg_15m_avg_1h,
        MIN(service_max_latency_ms_share) OVER (PARTITION BY key) AS key_min_service_max_latency_ms_share
    FROM filtered
),
metrics_app_logs AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        v,
        latency_ms_adjusted,
        key_filled,
        resolved_key,
        latency_ms_avg_15m,
        service_stddev_samp_latency_ms_adjusted,
        latency_ms_share,
        level_stddev_samp_latency_ms_adjusted,
        service_max_latency_ms_share,
        service_stddev_samp_latency_ms_adjusted_dense_rank,
        service_stddev_samp_latency_ms_adjusted_dense_rank_filled,
        resolved_key_2,
        service_max_latency_ms_share_count_15m,
        latency_ms_avg_15m_avg_1h,
        key_min_service_max_latency_ms_share,
        AVG(key_min_service_max_latency_ms_share) OVER (PARTITION BY key, host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS key_min_service_max_latency_ms_share_avg_30m,
        COUNT(latency_ms_avg_15m_avg_1h) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_avg_15m_avg_1h_count_1h
    FROM enriched
)
-- Window metrics over several different partitions
SELECT
    *,
    MIN(latency_ms_avg_15m_avg_1h) OVER (PARTITION BY level, host) AS level_min_latency_ms_avg_15m_avg_1h,
    CUME_DIST() OVER (PARTITION BY key ORDER BY service_max_latency_ms_share DESC) AS service_max_latency_ms_share_cume_dist
FROM metrics_app_logs
