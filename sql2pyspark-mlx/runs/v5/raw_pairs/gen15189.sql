WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, v
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CEIL(latency_ms) AS latency_ms_whole,
        COALESCE(latency_ms, 0) AS latency_ms_filled,
        COALESCE(k, 'UNKNOWN') AS k_clean
    FROM exploded
    WHERE k = 'trace'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        host,
        latency_ms,
        v,
        latency_ms_whole,
        k_clean,
        MIN(latency_ms_filled) OVER (PARTITION BY level) AS level_min_latency_ms_filled
    FROM base
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT *, STDDEV_SAMP(latency_ms_whole) OVER (PARTITION BY host) AS host_stddev_samp_latency_ms_whole
    FROM enriched
    WHERE latency_ms = 1580
),
with_windows_app_logs AS (
    -- Window metrics over several different partitions
    SELECT *, MIN(latency_ms_whole) OVER (PARTITION BY host) AS host_min_latency_ms_whole
    FROM with_windows
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(latency_ms_whole, 0) AS latency_ms_whole_filled,
        host_min_latency_ms_whole - host_stddev_samp_latency_ms_whole AS host_min_latency_ms_whole_minus_host_stddev_samp_latency_ms_whole
    FROM with_windows_app_logs
    WHERE host_min_latency_ms_whole != 34
),
enriched_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY host ORDER BY host_min_latency_ms_whole DESC NULLS FIRST) AS host_min_latency_ms_whole_cume_dist,
        DENSE_RANK() OVER (PARTITION BY host ORDER BY host_min_latency_ms_whole DESC NULLS LAST) AS host_min_latency_ms_whole_dense_rank
    FROM valid
)
-- Multi-level aggregation
SELECT
    host,
    CASE WHEN host_stddev_samp_latency_ms_whole >= 73 THEN 'HIGH' ELSE 'LOW' END AS host_stddev_samp_latency_ms_whole_tier,
    COUNT(*) AS row_count,
    COUNT_IF(host = 'h3') AS h3_events,
    SUM(host_min_latency_ms_whole_minus_host_stddev_samp_latency_ms_whole) AS total_host_min_latency_ms_whole_minus_host_stddev_samp_latency_ms_whole
FROM enriched_app_logs
GROUP BY ROLLUP (host, CASE WHEN host_stddev_samp_latency_ms_whole >= 73 THEN 'HIGH' ELSE 'LOW' END)
HAVING COUNT(*) < 1000
