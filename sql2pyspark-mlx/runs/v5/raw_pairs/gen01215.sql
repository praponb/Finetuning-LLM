WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, attr_value
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        key,
        attr_value,
        CORR(status_code, latency_ms) OVER (PARTITION BY level) AS corr_status_code_latency_ms,
        CUME_DIST() OVER (ORDER BY status_code) AS status_code_cume_dist
    FROM lines
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_count_1h,
        MIN(status_code_cume_dist) OVER (PARTITION BY level) AS level_min_status_code_cume_dist,
        RANK() OVER (ORDER BY status_code_cume_dist DESC NULLS LAST) AS status_code_cume_dist_rank,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_dense_rank,
        SUM(status_code_cume_dist) OVER (PARTITION BY key, level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_cume_dist_sum_2h
    FROM with_windows
),
metrics_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(status_code_cume_dist_sum_2h) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_cume_dist_sum_2h_count_1h,
        ROUND(latency_ms_count_1h / SUM(latency_ms_count_1h) OVER (PARTITION BY key), 4) AS latency_ms_count_1h_share,
        CUME_DIST() OVER (ORDER BY latency_ms_count_1h DESC) AS latency_ms_count_1h_cume_dist
    FROM metrics
),
scored AS (
    -- Ranking and gap features
    SELECT
        latency_ms,
        key,
        attr_value,
        corr_status_code_latency_ms,
        latency_ms_count_1h,
        level_min_status_code_cume_dist,
        status_code_cume_dist_rank,
        latency_ms_count_1h_share,
        latency_ms_count_1h_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY level ORDER BY latency_ms_count_1h DESC NULLS FIRST) AS latency_ms_count_1h_percent_rank,
        MAX(latency_ms_count_1h_share) OVER w_recent AS level_max_latency_ms_count_1h_share,
        STDDEV_SAMP(latency_ms_dense_rank) OVER w_recent AS level_stddev_samp_latency_ms_dense_rank
    FROM metrics_app_logs
    WINDOW w_recent AS (PARTITION BY level)
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN latency_ms_count_1h >= 16 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_count_1h_tier,
        CASE WHEN level_min_status_code_cume_dist >= 1 THEN 'LARGE' ELSE 'SMALL' END AS level_min_status_code_cume_dist_tier,
        SIZE(COLLECT_SET(key)) AS num_keys,
        COUNT(*) FILTER (WHERE key = 'trace') AS trace_rows,
        BIT_AND(latency_ms) AS latency_ms_bit_and
    FROM scored
    GROUP BY GROUPING SETS ((CASE WHEN latency_ms_count_1h >= 16 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN level_min_status_code_cume_dist >= 1 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN latency_ms_count_1h >= 16 THEN 'PREMIUM' ELSE 'STANDARD' END))
)
SELECT
    latency_ms_count_1h_tier,
    num_keys,
    trace_rows
FROM stats
ORDER BY num_keys;
