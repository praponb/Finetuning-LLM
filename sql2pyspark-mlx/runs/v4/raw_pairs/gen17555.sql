WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level, service), 4) latency_ms_share,
        MAX(latency_ms) OVER w_grp level_max_latency_ms,
        STDDEV_SAMP(latency_ms) OVER w_grp level_stddev_samp_latency_ms,
        SUM(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) latency_ms_sum_1h,
        RANK() OVER (PARTITION BY service, level ORDER BY latency_ms DESC NULLS FIRST) latency_ms_rank
    FROM exploded
    WINDOW w_grp AS (PARTITION BY level)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY latency_ms_sum_1h DESC) latency_ms_sum_1h_percent_rank,
        MAX(level_max_latency_ms) OVER (PARTITION BY service, key) service_max_level_max_latency_ms,
        SUM(latency_ms_share) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) latency_ms_share_sum_30m
    FROM enriched
),
with_windows_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY level), 4) latency_ms_share_2,
        SUM(level_stddev_samp_latency_ms) OVER (PARTITION BY service, level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) level_stddev_samp_latency_ms_sum_15m,
        ROUND(level_max_latency_ms / SUM(level_max_latency_ms) OVER (PARTITION BY service, level), 4) level_max_latency_ms_share
    FROM with_windows
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, latency_ms_sum_1h * latency_ms_sum_1h_percent_rank latency_ms_sum_1h_times_latency_ms_sum_1h_percent_rank
    FROM with_windows_app_logs
    WHERE log_ts > DATE '2025-06-01'
      AND key IS NOT NULL
      AND latency_ms = 1022
),
enriched_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY level_max_latency_ms_share DESC NULLS LAST) level_max_latency_ms_share_dense_rank,
        MAX(level_max_latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) level_max_latency_ms_max_15m,
        MAX(latency_ms_sum_1h_times_latency_ms_sum_1h_percent_rank) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) latency_ms_sum_1h_times_latency_ms_sum_1h_percent_rank_max_2h,
        CUME_DIST() OVER (ORDER BY service_max_level_max_latency_ms DESC NULLS LAST) service_max_level_max_latency_ms_cume_dist
    FROM valid
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('HOUR', log_ts) AS hour_window,
        CASE WHEN level_max_latency_ms_share_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS level_max_latency_ms_share_dense_rank_tier,
        (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS duration_min,
        APPROX_PERCENTILE(level_stddev_samp_latency_ms, ARRAY(0.25, 0.5, 0.75)) AS level_stddev_samp_latency_ms_quartiles,
        COUNT(CASE WHEN key = 'region' THEN 1 END) AS region_count,
        BIT_OR(latency_ms) AS latency_ms_bit_or
    FROM enriched_app_logs
    GROUP BY CUBE (DATE_TRUNC('HOUR', log_ts), CASE WHEN level_max_latency_ms_share_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT
    level_max_latency_ms_share_dense_rank_tier,
    duration_min,
    level_stddev_samp_latency_ms_quartiles,
    region_count,
    latency_ms_bit_or
FROM daily
ORDER BY duration_min, region_count;
