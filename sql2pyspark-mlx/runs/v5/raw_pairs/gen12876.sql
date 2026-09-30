WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        log_ts,
        key,
        attr_value,
        AVG(latency_ms) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_avg_2h,
        STDDEV_SAMP(latency_ms) OVER (PARTITION BY key) AS key_stddev_samp_latency_ms,
        MIN(latency_ms) OVER (PARTITION BY level) AS level_min_latency_ms,
        SUM(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_sum_15m
    FROM lines
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        latency_ms_sum_15m * level_min_latency_ms AS latency_ms_sum_15m_times_level_min_latency_ms,
        COALESCE(key, 'UNKNOWN') AS key_clean
    FROM scored
    WHERE key <> 'region'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(latency_ms_sum_15m_times_level_min_latency_ms) OVER w_ordered AS key_stddev_samp_latency_ms_sum_15m_times_level_min_latency_ms,
        PERCENT_RANK() OVER (PARTITION BY key ORDER BY latency_ms_sum_15m_times_level_min_latency_ms DESC NULLS LAST) AS latency_ms_sum_15m_times_level_min_latency_ms_percent_rank,
        AVG(key_stddev_samp_latency_ms) OVER w_ordered AS key_avg_key_stddev_samp_latency_ms,
        CUME_DIST() OVER w_grp AS latency_ms_sum_15m_times_level_min_latency_ms_cume_dist,
        COUNT(latency_ms_avg_2h) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_avg_2h_count_1h
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY key), w_grp AS (PARTITION BY key ORDER BY latency_ms_sum_15m_times_level_min_latency_ms DESC)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(latency_ms_sum_15m_times_level_min_latency_ms_cume_dist / key_stddev_samp_latency_ms, 2) AS latency_ms_sum_15m_times_level_min_latency_ms_cume_dist_per_key_stddev_samp_latency_ms,
        TO_DATE(log_ts) AS log_ts_date,
        COALESCE(key, 'OTHER') AS key_filled
    FROM enriched
    WHERE key IS NOT NULL
      AND key IS DISTINCT FROM 'region'
)
-- Multi-level aggregation
SELECT
    CASE WHEN level_min_latency_ms >= 1875 THEN 'PREMIUM' ELSE 'STANDARD' END AS level_min_latency_ms_tier,
    CASE WHEN latency_ms_sum_15m_times_level_min_latency_ms_cume_dist >= 0.63 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_sum_15m_times_level_min_latency_ms_cume_dist_tier,
    SIZE(COLLECT_SET(key)) AS num_keys,
    COUNT(*) FILTER (WHERE key = 'trace') AS trace_rows,
    MAX(log_ts) AS last_log_ts,
    PERCENTILE_APPROX(key_stddev_samp_latency_ms_sum_15m_times_level_min_latency_ms, ARRAY(0.25, 0.5, 0.75)) AS key_stddev_samp_latency_ms_sum_15m_times_level_min_latency_ms_quartiles,
    MIN(log_ts) AS first_seen,
    COUNT(CASE WHEN key = 'trace' THEN 1 END) AS trace_count,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN level_min_latency_ms >= 1875 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SIZE(COLLECT_SET(key)) DESC) AS num_keys_percent_rank
FROM valid
GROUP BY CASE WHEN level_min_latency_ms >= 1875 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN latency_ms_sum_15m_times_level_min_latency_ms_cume_dist >= 0.63 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY level_min_latency_ms_tier NULLS LAST;
