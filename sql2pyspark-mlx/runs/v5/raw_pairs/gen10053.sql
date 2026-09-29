WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        level,
        key,
        v,
        CASE WHEN host = 'h1' THEN 1 ELSE 0 END AS is_h1,
        CONCAT(key, ' ', host) AS key_host,
        COALESCE(key, 'N/A') AS resolved_key
    FROM lines
    WHERE key IS NOT NULL
      AND key IN ('version', 'region', 'trace')
),
filtered_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        level,
        v,
        is_h1,
        key_host,
        ROUND(is_h1 * 1.07, 2) AS is_h1_adjusted
    FROM filtered
    WHERE key IS NOT DISTINCT FROM 'region'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY is_h1 DESC) AS is_h1_dense_rank,
        DENSE_RANK() OVER (ORDER BY is_h1 DESC NULLS LAST) AS is_h1_dense_rank_2,
        MIN(is_h1) OVER (PARTITION BY level) AS level_min_is_h1,
        MAX(is_h1_adjusted) OVER (PARTITION BY level) AS level_max_is_h1_adjusted,
        RANK() OVER (ORDER BY is_h1 NULLS FIRST) AS is_h1_rank
    FROM filtered_app_logs
)
-- Grouped metrics
SELECT
    host,
    level,
    COUNT(DISTINCT level) AS distinct_levels,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    PERCENTILE_APPROX(is_h1_rank, ARRAY(0.25, 0.5, 0.75)) AS is_h1_rank_quartiles,
    ROUND(SUM(level_min_is_h1) / NULLIF(COUNT(CASE WHEN level = 'DEBUG' THEN 1 END), 0), 2) AS level_min_is_h1_per_debug,
    GROUPING(host) AS gid
FROM metrics
GROUP BY ROLLUP (host, level)
HAVING level_min_is_h1_per_debug < 1000000
ORDER BY host DESC
