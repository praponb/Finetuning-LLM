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
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(host)) AS host_title,
        CASE WHEN latency_ms >= 2333 THEN 'High' WHEN latency_ms >= 1141 THEN 'Medium' ELSE 'Low' END AS latency_ms_level,
        COALESCE(key, 'OTHER') AS resolved_key
    FROM lines
    WHERE service IN ('checkout', 'billing')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_avg_30m,
        MAX(status_code) OVER (PARTITION BY host, service) AS host_max_status_code,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host), 4) AS latency_ms_share,
        COUNT(status_code) OVER (PARTITION BY service, key ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_count_1h,
        AVG(status_code) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_avg_30m_2
    FROM valid
    WHERE key <> 'trace'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        v,
        host_title,
        latency_ms_level,
        resolved_key,
        status_code_avg_30m,
        host_max_status_code,
        latency_ms_share,
        status_code_count_1h,
        CONCAT(level, '/', key) AS level_key,
        COALESCE(key, 'OTHER') AS key_clean
    FROM with_windows
    WHERE status_code BETWEEN 274 AND 412
),
with_windows_app_logs AS (
    -- Ranking and gap features
    SELECT
        service,
        host,
        log_ts,
        v,
        host_title,
        host_max_status_code,
        latency_ms_share,
        status_code_count_1h,
        level_key,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service), 4) AS latency_ms_share_2,
        DENSE_RANK() OVER (PARTITION BY host ORDER BY status_code_avg_30m DESC) AS status_code_avg_30m_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY service ORDER BY latency_ms ASC NULLS FIRST) AS latency_ms_percent_rank
    FROM filtered
),
daily AS (
    -- Grouped metrics
    SELECT
        service,
        DATE_TRUNC('HOUR', log_ts) AS hour_window,
        DATE_TRUNC('DAY', log_ts) AS activity_day,
        PERCENTILE_APPROX(latency_ms_share, ARRAY(0.25, 0.5, 0.75)) AS latency_ms_share_quartiles,
        MAX(status_code_avg_30m_dense_rank) AS max_status_code_avg_30m_dense_rank,
        (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
        COUNT(*) AS cnt,
        APPROX_PERCENTILE(status_code_avg_30m_dense_rank, 0.9) AS status_code_avg_30m_dense_rank_p90,
        ROUND(SUM(latency_ms_share) / NULLIF(COUNT(CASE WHEN host = 'h2' THEN 1 END), 0), 2) AS latency_ms_share_per_h2
    FROM with_windows_app_logs
    GROUP BY service, DATE_TRUNC('HOUR', log_ts), DATE_TRUNC('DAY', log_ts)
)
SELECT
    service,
    hour_window,
    activity_day,
    latency_ms_share_quartiles,
    max_status_code_avg_30m_dense_rank,
    span_minutes,
    cnt,
    latency_ms_share_per_h2,
    CASE WHEN service RLIKE '^(auth|checkout)$' THEN 'OTHER_ZONE' WHEN service RLIKE '^(billing|search)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS service_zone
FROM daily
ORDER BY cnt DESC;
