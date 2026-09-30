WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, tag
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS tag
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(tag) AS tag_length,
        COALESCE(tag, 'DIRECT') AS tag_clean
    FROM flattened
    WHERE tag IS NOT NULL
      AND tag = 'SPRING10'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(tag_length AS DOUBLE) AS tag_length_str,
        COALESCE(tag, 'DIRECT') AS resolved_tag
    FROM valid
    WHERE tag = 'VIP'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY tag_length DESC NULLS FIRST) AS tag_length_rank,
        PERCENT_RANK() OVER (PARTITION BY tag ORDER BY tag_length DESC NULLS LAST) AS tag_length_percent_rank
    FROM base
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        tag,
        tag_length,
        tag_clean,
        resolved_tag,
        tag_length_rank,
        ROUND(tag_length_rank / SUM(tag_length_rank) OVER (PARTITION BY tag), 4) AS tag_length_rank_share,
        CUME_DIST() OVER (PARTITION BY tag ORDER BY tag_length_str) AS tag_length_str_cume_dist,
        PERCENT_RANK() OVER (ORDER BY tag_length NULLS FIRST) AS tag_length_percent_rank_2
    FROM with_windows
)
SELECT
    tag,
    tag_length,
    tag_clean,
    resolved_tag,
    tag_length_rank_share,
    tag_length_percent_rank_2
FROM enriched
ORDER BY tag_length NULLS LAST;
