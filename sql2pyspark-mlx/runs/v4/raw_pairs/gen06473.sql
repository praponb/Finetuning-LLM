WITH lines AS (
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
    LATERAL VIEW OUTER EXPLODE(context) li AS key, value
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) AS status_code_share,
        MAX(latency_ms) OVER w_ordered AS latency_ms_max_15m,
        COUNT(latency_ms) OVER w_running AS latency_ms_count_15m
    FROM lines
    WINDOW w_ordered AS (PARTITION BY level, host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY host ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_dense_rank,
        ROUND(latency_ms_count_15m / SUM(latency_ms_count_15m) OVER (PARTITION BY host), 4) AS latency_ms_count_15m_share,
        COUNT(status_code) OVER (PARTITION BY host, key) AS host_count_status_code
    FROM enriched
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        key,
        value,
        status_code_share,
        latency_ms_max_15m,
        latency_ms_count_15m,
        latency_ms_count_15m_share,
        host_count_status_code,
        ROUND(latency_ms_count_15m_share * 1.15, 2) AS latency_ms_count_15m_share_scaled
    FROM with_windows
    WHERE latency_ms_max_15m > 1890
),
with_windows_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(latency_ms_count_15m) OVER (PARTITION BY key) AS key_stddev_samp_latency_ms_count_15m,
        CUME_DIST() OVER (PARTITION BY service, level ORDER BY latency_ms_count_15m_share_scaled DESC NULLS LAST) AS latency_ms_count_15m_share_scaled_cume_dist
    FROM valid
),
enriched_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_hist AS status_code_share_rank,
        RANK() OVER (PARTITION BY key, service ORDER BY latency_ms_count_15m_share NULLS FIRST) AS latency_ms_count_15m_share_rank,
        COUNT(key_stddev_samp_latency_ms_count_15m) OVER (PARTITION BY level, key) AS level_count_key_stddev_samp_latency_ms_count_15m
    FROM with_windows_app_logs
    WINDOW w_hist AS (ORDER BY status_code_share NULLS LAST)
)
SELECT
    id,
    service,
    level,
    latency_ms,
    key,
    value,
    status_code_share,
    latency_ms_count_15m,
    latency_ms_count_15m_share_scaled
FROM enriched_app_logs
ORDER BY key DESC NULLS FIRST;
