WITH lines AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE_OUTER(coupon_codes) AS tag
    FROM carts
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(tag) tag_lower,
        LENGTH(tag) tag_length,
        COALESCE(tag, 'N/A') resolved_tag
    FROM lines
    WHERE tag IN ('VIP', 'WELCOME5', 'SPRING10')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY tag ORDER BY tag_length DESC) tag_length_dense_rank,
        CUME_DIST() OVER (ORDER BY tag_length NULLS FIRST) tag_length_cume_dist,
        MIN(tag_length) OVER (PARTITION BY tag) tag_min_tag_length
    FROM valid
),
metrics_carts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY tag ORDER BY tag_length_cume_dist DESC NULLS FIRST) tag_length_cume_dist_dense_rank,
        DENSE_RANK() OVER (PARTITION BY tag ORDER BY tag_min_tag_length) tag_min_tag_length_dense_rank,
        DENSE_RANK() OVER (ORDER BY tag_length_dense_rank DESC NULLS LAST) tag_length_dense_rank_dense_rank
    FROM metrics
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(tag_length_dense_rank_dense_rank, tag_length_dense_rank) greatest_tag_length_dense_rank_dense_rank_tag_length_dense_rank
    FROM metrics_carts
    WHERE tag_length_dense_rank BETWEEN 6 AND 8
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY tag ORDER BY tag_length) tag_length_dense_rank_2,
        RANK() OVER (ORDER BY tag_length_dense_rank DESC NULLS LAST) tag_length_dense_rank_rank,
        CUME_DIST() OVER (ORDER BY tag_length_dense_rank DESC NULLS LAST) tag_length_dense_rank_cume_dist,
        MAX(tag_length) OVER (PARTITION BY tag) tag_max_tag_length
    FROM filtered
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        GREATEST(tag_length_cume_dist, greatest_tag_length_dense_rank_dense_rank_tag_length_dense_rank) greatest_tag_length_cume_dist_greatest_tag_length_dense_rank_dense_rank_tag_length_dense_rank,
        COALESCE(tag, 'UNKNOWN') tag_filled
    FROM scored
    WHERE tag IS NOT NULL
)
-- Grouped metrics
SELECT
    tag,
    SUM(CASE WHEN tag = 'BUNDLE' THEN tag_length_dense_rank_2 ELSE 0 END) AS bundle_tag_length_dense_rank_2,
    ROUND(AVG(tag_length_dense_rank_rank), 2) AS avg_tag_length_dense_rank_rank,
    SUM(CASE WHEN tag = 'VIP' THEN tag_length_cume_dist ELSE 0 END) AS vip_tag_length_cume_dist,
    SORT_ARRAY(COLLECT_SET(tag)) AS tag_list,
    PERCENTILE_APPROX(tag_length_dense_rank_cume_dist, 0.9) AS tag_length_dense_rank_cume_dist_p90
FROM base
GROUP BY tag
HAVING bundle_tag_length_dense_rank_2 >= 0;
