WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, tag
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS tag
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM flattened
    EXCEPT
    SELECT id
    FROM carts
    WHERE customer_id IN (11, 15, 35)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    JOIN matched_keys k ON s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(tag) clean_tag
    FROM kept_rows
    WHERE tag IS NOT NULL
      AND tag NOT IN ('VIP', 'BUNDLE')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN tag = 'BUNDLE' THEN 1 ELSE 0 END is_bundle
    FROM base
    WHERE tag <> 'BUNDLE'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        tag,
        clean_tag,
        is_bundle,
        COUNT(is_bundle) OVER w_recent tag_count_is_bundle,
        CUME_DIST() OVER (ORDER BY is_bundle DESC NULLS FIRST) is_bundle_cume_dist
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY tag)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        tag,
        clean_tag,
        is_bundle,
        tag_count_is_bundle,
        is_bundle_cume_dist,
        DENSE_RANK() OVER (ORDER BY is_bundle DESC NULLS FIRST) is_bundle_dense_rank,
        RANK() OVER (PARTITION BY tag ORDER BY is_bundle_cume_dist ASC NULLS FIRST) is_bundle_cume_dist_rank,
        PERCENT_RANK() OVER (PARTITION BY tag ORDER BY is_bundle DESC NULLS LAST) is_bundle_percent_rank
    FROM with_windows
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        tag,
        clean_tag,
        is_bundle,
        is_bundle_cume_dist,
        is_bundle_cume_dist_rank,
        is_bundle_percent_rank,
        ROUND(is_bundle_cume_dist * 0.9, 2) adjusted_is_bundle_cume_dist,
        COALESCE(tag, 'N/A') resolved_tag
    FROM scored
    WHERE tag = 'SPRING10'
),
summary AS (
    -- Multi-level aggregation
    SELECT
        tag,
        AVG(is_bundle_cume_dist_rank) AS avg_is_bundle_cume_dist_rank,
        ROUND(AVG(is_bundle_cume_dist), 2) AS avg_is_bundle_cume_dist
    FROM valid
    GROUP BY tag
)
SELECT
    tag,
    avg_is_bundle_cume_dist_rank,
    avg_is_bundle_cume_dist
FROM summary;
