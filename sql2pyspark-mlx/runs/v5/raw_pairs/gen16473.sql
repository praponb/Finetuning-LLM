WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        tag
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) t AS tag
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, YEAR(created_at) AS created_at_year
    FROM flattened
    WHERE created_at > DATE '2025-03-01'
      AND TO_DATE(created_at) >= '2023-12-01'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, COUNT(created_at_year) OVER (PARTITION BY tag) AS tag_count_created_at_year
    FROM base
    WHERE tag IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(tag_count_created_at_year * 1.15, 2) AS adjusted_tag_count_created_at_year,
        COALESCE(tag, 'N/A') AS tag_filled
    FROM scored
    WHERE created_at > DATE '2025-03-01'
      AND created_at >= '2024-03-14'
),
cleaned_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(created_at_year AS DOUBLE) AS created_at_year_num,
        ROUND(created_at_year / tag_count_created_at_year, 2) AS created_at_year_per_tag_count_created_at_year
    FROM cleaned
    WHERE created_at_year BETWEEN 54 AND 67
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        tag,
        tag_count_created_at_year,
        adjusted_tag_count_created_at_year,
        tag_filled,
        created_at_year_per_tag_count_created_at_year,
        SUM(created_at_year) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS created_at_year_sum_1h,
        MIN(created_at_year_num) OVER (PARTITION BY tag) AS tag_min_created_at_year_num,
        CUME_DIST() OVER (PARTITION BY tag ORDER BY created_at_year_num DESC NULLS LAST) AS created_at_year_num_cume_dist,
        RANK() OVER (ORDER BY created_at_year_per_tag_count_created_at_year DESC NULLS LAST) AS created_at_year_per_tag_count_created_at_year_rank
    FROM cleaned_carts
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        tag,
        tag_count_created_at_year,
        adjusted_tag_count_created_at_year,
        created_at_year_per_tag_count_created_at_year,
        created_at_year_sum_1h,
        CAST(created_at_year_per_tag_count_created_at_year_rank AS DOUBLE) AS created_at_year_per_tag_count_created_at_year_rank_num,
        created_at_year_num_cume_dist / NULLIF(created_at_year_per_tag_count_created_at_year, 0) AS created_at_year_num_cume_dist_per_created_at_year_per_tag_count_created_at_year,
        COALESCE(tag, 'DIRECT') AS tag_filled_2
    FROM metrics
    WHERE tag IS NOT NULL
      AND created_at_year_sum_1h <= 297
)
SELECT
    tag,
    tag_count_created_at_year,
    adjusted_tag_count_created_at_year,
    created_at_year_per_tag_count_created_at_year,
    created_at_year_num_cume_dist_per_created_at_year_per_tag_count_created_at_year
FROM valid
ORDER BY created_at_year_per_tag_count_created_at_year DESC, tag
