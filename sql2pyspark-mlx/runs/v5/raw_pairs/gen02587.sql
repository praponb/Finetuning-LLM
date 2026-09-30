WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, v
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(level, 1, 2) AS level_prefix,
        CAST(status_code AS DOUBLE) AS status_code_num,
        COALESCE(k, 'N/A') AS k_filled
    FROM lines
    WHERE k IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN level = 'DEBUG' THEN 1 ELSE 0 END AS is_debug
    FROM base
    WHERE k IS NOT NULL
      AND k NOT IN ('trace', 'region')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(status_code_num * 100, 2) AS status_code_num_adjusted
    FROM valid
    WHERE k IS DISTINCT FROM 'version'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        level,
        status_code,
        k,
        status_code_num,
        k_filled,
        is_debug,
        status_code_num_adjusted,
        status_code_num_adjusted - status_code_num AS status_code_num_adjusted_minus_status_code_num
    FROM cleaned
    WHERE level = 'DEBUG'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        level,
        k,
        k_filled,
        is_debug,
        status_code_num_adjusted,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY status_code DESC) AS status_code_dense_rank,
        CUME_DIST() OVER (ORDER BY is_debug DESC NULLS FIRST) AS is_debug_cume_dist,
        COUNT(status_code_num_adjusted) OVER (PARTITION BY level) AS level_count_status_code_num_adjusted,
        CUME_DIST() OVER (PARTITION BY k ORDER BY is_debug) AS is_debug_cume_dist_2
    FROM filtered
)
SELECT
    id,
    level,
    k,
    k_filled,
    is_debug,
    status_code_num_adjusted,
    status_code_dense_rank,
    is_debug_cume_dist,
    level_count_status_code_num_adjusted
FROM metrics
ORDER BY id DESC
