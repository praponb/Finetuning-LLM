WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE_OUTER(coupon_codes) AS tag
    FROM carts
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(tag, 1, 3) AS tag_prefix
    FROM flattened
    WHERE tag IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN tag = 'BUNDLE' THEN 1 ELSE 0 END AS is_bundle,
        COALESCE(tag, 'DIRECT') AS resolved_tag
    FROM filtered
    WHERE tag IN ('SPRING10', 'WELCOME5', 'VIP')
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM cleaned
    EXCEPT ALL
    SELECT id
    FROM carts
    WHERE created_at >= '2024-09-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.id = k.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        tag,
        tag_prefix,
        is_bundle,
        resolved_tag,
        CUME_DIST() OVER (ORDER BY is_bundle DESC NULLS LAST) AS is_bundle_cume_dist,
        DENSE_RANK() OVER w_running AS is_bundle_dense_rank,
        CUME_DIST() OVER w_running AS is_bundle_cume_dist_2
    FROM verified
    WINDOW w_running AS (PARTITION BY tag ORDER BY is_bundle NULLS FIRST)
),
filtered_carts AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        tag_prefix,
        resolved_tag,
        is_bundle_cume_dist,
        is_bundle_dense_rank,
        is_bundle_cume_dist_2,
        CASE WHEN tag IN ('WELCOME5', 'VIP') THEN 'Core' ELSE 'Other' END AS tag_group,
        CAST(is_bundle_cume_dist_2 AS INT) AS is_bundle_cume_dist_2_int
    FROM scored
    WHERE tag IS NOT NULL
),
scored_carts AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY is_bundle_cume_dist_2_int) AS is_bundle_cume_dist_2_int_cume_dist,
        RANK() OVER (ORDER BY is_bundle_cume_dist DESC NULLS LAST) AS is_bundle_cume_dist_rank,
        CUME_DIST() OVER (ORDER BY is_bundle_cume_dist_2 DESC NULLS LAST) AS is_bundle_cume_dist_2_cume_dist
    FROM filtered_carts
)
SELECT
    id,
    resolved_tag,
    is_bundle_cume_dist,
    is_bundle_dense_rank,
    is_bundle_cume_dist_2,
    tag_group,
    is_bundle_cume_dist_2_int,
    is_bundle_cume_dist_2_int_cume_dist,
    is_bundle_cume_dist_rank
FROM scored_carts
ORDER BY is_bundle_cume_dist_rank NULLS LAST;
