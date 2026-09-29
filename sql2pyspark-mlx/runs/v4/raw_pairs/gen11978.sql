WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, attr_value
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN k = 'region' THEN 1 ELSE 0 END AS is_region,
        LOWER(k) AS k_lower
    FROM lines
    WHERE k IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY is_region NULLS LAST) AS is_region_cume_dist,
        STDDEV_SAMP(is_region) OVER (PARTITION BY level, host) AS level_stddev_samp_is_region,
        STDDEV_SAMP(is_region) OVER (PARTITION BY k) AS k_stddev_samp_is_region,
        PERCENT_RANK() OVER (ORDER BY is_region DESC NULLS LAST) AS is_region_percent_rank
    FROM valid
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        level,
        k,
        attr_value,
        is_region,
        k_lower,
        is_region_cume_dist,
        level_stddev_samp_is_region,
        k_stddev_samp_is_region,
        is_region_percent_rank,
        LEAST(k_stddev_samp_is_region, level_stddev_samp_is_region) AS least_k_stddev_samp_is_region_level_stddev_samp_is_region,
        COALESCE(k, 'DIRECT') AS resolved_k
    FROM metrics
    WHERE k IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER w_ordered AS k_stddev_samp_is_region_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY host, k ORDER BY is_region_cume_dist) AS is_region_cume_dist_percent_rank,
        MIN(is_region) OVER (PARTITION BY host, level) AS host_min_is_region
    FROM base
    WINDOW w_ordered AS (PARTITION BY k ORDER BY k_stddev_samp_is_region DESC NULLS LAST)
),
base_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        is_region_cume_dist + k_stddev_samp_is_region_dense_rank AS is_region_cume_dist_plus_k_stddev_samp_is_region_dense_rank,
        COALESCE(k, 'NONE') AS resolved_k_2
    FROM with_windows
    WHERE k IS NOT NULL
      AND level_stddev_samp_is_region IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT level, host
    FROM base_app_logs
    EXCEPT
    SELECT level, host
    FROM app_logs
    WHERE status_code < 276
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base_app_logs s
    INNER JOIN remaining_keys k ON s.level = k.level AND s.host = k.host
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        level,
        k,
        is_region_cume_dist_percent_rank
    FROM verified
) src
PIVOT (
    MAX(is_region_cume_dist_percent_rank)
    FOR k IN ('trace', 'region')
);
