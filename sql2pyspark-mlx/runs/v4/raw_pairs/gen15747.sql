WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, v
),
with_windows AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        key,
        v,
        COUNT(latency_ms) OVER (PARTITION BY host) AS host_count_latency_ms,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY service, host), 4) AS latency_ms_share,
        MAX(latency_ms) OVER (PARTITION BY key) AS key_max_latency_ms,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY key), 4) AS latency_ms_share_2
    FROM flattened
),
with_windows_app_logs AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        key,
        v,
        host_count_latency_ms,
        latency_ms_share,
        key_max_latency_ms,
        latency_ms_share_2,
        DENSE_RANK() OVER w_recent AS host_count_latency_ms_dense_rank,
        STDDEV_SAMP(latency_ms_share) OVER (PARTITION BY host, key) AS host_stddev_samp_latency_ms_share
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY service, host ORDER BY host_count_latency_ms NULLS LAST)
),
metrics AS (
    SELECT
        *,
        MIN(key_max_latency_ms) OVER (PARTITION BY service) AS service_min_key_max_latency_ms,
        DENSE_RANK() OVER w_hist AS key_max_latency_ms_dense_rank,
        AVG(key_max_latency_ms) OVER (PARTITION BY key, host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS key_max_latency_ms_avg_1h
    FROM with_windows_app_logs
    WINDOW w_hist AS (PARTITION BY host ORDER BY key_max_latency_ms DESC NULLS FIRST)
),
filtered AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        key,
        v,
        key_max_latency_ms,
        latency_ms_share_2,
        host_count_latency_ms_dense_rank,
        service_min_key_max_latency_ms,
        key_max_latency_ms_dense_rank,
        key_max_latency_ms_avg_1h,
        UPPER(host) AS host_upper,
        CONCAT(service, ', ', host) AS service_host,
        COALESCE(key, 'DIRECT') AS key_filled
    FROM metrics
    WHERE log_ts > DATE '2025-06-02'
),
cleaned AS (
    SELECT
        *,
        CONCAT(service, '/', key) AS service_key,
        CASE WHEN service IN ('auth', 'checkout') THEN 'Group A' ELSE 'Other' END AS service_group
    FROM filtered
    WHERE key IS NOT NULL
      AND service = 'billing'
),
filtered_app_logs AS (
    SELECT
        *,
        LOWER(key) AS key_lower,
        key_max_latency_ms_dense_rank * latency_ms_share_2 AS key_max_latency_ms_dense_rank_times_latency_ms_share_2
    FROM cleaned
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 12:00:00')
      AND key IS NOT NULL
      AND TO_DATE(log_ts) < '2024-12-01'
)
SELECT
    service,
    MIN(log_ts) AS started_at,
    COUNT(*) FILTER (WHERE service = 'search') AS search_rows,
    ROUND(SUM(key_max_latency_ms_dense_rank) / NULLIF(COUNT(CASE WHEN host = 'h1' THEN 1 END), 0), 2) AS key_max_latency_ms_dense_rank_per_h1,
    PERCENTILE_APPROX(key_max_latency_ms_dense_rank_times_latency_ms_share_2, ARRAY(0.25, 0.5, 0.75)) AS key_max_latency_ms_dense_rank_times_latency_ms_share_2_quartiles,
    COUNT(DISTINCT service) AS distinct_services
FROM filtered_app_logs
GROUP BY service
HAVING key_max_latency_ms_dense_rank_per_h1 >= 0
