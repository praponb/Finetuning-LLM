WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, v
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN k = 'region' THEN 1 ELSE 0 END AS is_region
    FROM exploded
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 14:00:00')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY k, host ORDER BY is_region DESC NULLS FIRST) AS is_region_dense_rank,
        STDDEV_SAMP(is_region) OVER (PARTITION BY host) AS host_stddev_samp_is_region,
        COUNT(is_region) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS is_region_count_15m,
        CUME_DIST() OVER (PARTITION BY k ORDER BY is_region DESC NULLS FIRST) AS is_region_cume_dist
    FROM base
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        host,
        log_ts,
        v,
        host_stddev_samp_is_region,
        is_region_count_15m,
        is_region_cume_dist,
        CAST(is_region_dense_rank AS STRING) AS is_region_dense_rank_num
    FROM scored
    WHERE log_ts > TIMESTAMP '2025-06-01 05:00:00'
      AND k IS NOT NULL
),
valid_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        host,
        log_ts,
        v,
        host_stddev_samp_is_region,
        is_region_count_15m,
        is_region_dense_rank_num,
        ROUND(is_region_cume_dist, 1) AS is_region_cume_dist_rounded
    FROM valid
    WHERE log_ts >= TIMESTAMP '2025-06-01 07:00:00'
),
enriched AS (
    -- Running and moving aggregates
    SELECT *, ROUND(is_region_count_15m / SUM(is_region_count_15m) OVER (PARTITION BY service, host), 4) AS is_region_count_15m_share
    FROM valid_app_logs
    WHERE host NOT IN ('h2', 'h1')
)
SELECT
    id,
    log_ts,
    host_stddev_samp_is_region,
    is_region_count_15m,
    is_region_dense_rank_num,
    is_region_cume_dist_rounded,
    is_region_count_15m_share
FROM enriched
