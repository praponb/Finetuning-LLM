WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, v
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_max_1h,
        SUM(status_code) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_sum_15m,
        COUNT(status_code) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_count_1h
    FROM exploded
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(status_code) OVER (PARTITION BY level) AS level_stddev_samp_status_code,
        SUM(status_code_max_1h) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_max_1h_sum_2h
    FROM with_windows
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        v,
        status_code_max_1h,
        status_code_sum_15m,
        status_code_count_1h,
        level_stddev_samp_status_code,
        status_code_max_1h_sum_2h,
        AVG(level_stddev_samp_status_code) OVER (PARTITION BY level, k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS level_stddev_samp_status_code_avg_15m,
        STDDEV_SAMP(status_code_count_1h) OVER (PARTITION BY host) AS host_stddev_samp_status_code_count_1h,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY status_code_max_1h DESC NULLS FIRST) AS status_code_max_1h_dense_rank
    FROM metrics
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN level_stddev_samp_status_code_avg_15m >= 469 THEN 'LARGE' ELSE 'SMALL' END AS level_stddev_samp_status_code_avg_15m_tier,
    level,
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    SIZE(COLLECT_SET(host)) AS num_hosts,
    COUNT(*) FILTER (WHERE k = 'version') AS version_rows,
    COUNT(*) FILTER (WHERE k = 'region') AS region_rows,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS duration_min,
    COUNT_IF(host = 'h1') AS h1_events,
    MAX(log_ts) AS ended_at,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN level_stddev_samp_status_code_avg_15m >= 469 THEN 'LARGE' ELSE 'SMALL' END ORDER BY SIZE(COLLECT_SET(host)) DESC) AS num_hosts_percent_rank
FROM enriched
GROUP BY CASE WHEN level_stddev_samp_status_code_avg_15m >= 469 THEN 'LARGE' ELSE 'SMALL' END, level, DATE_TRUNC('HOUR', log_ts)
