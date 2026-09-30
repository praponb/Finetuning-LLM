WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS element
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        element,
        RANK() OVER (ORDER BY element DESC NULLS FIRST) AS element_rank,
        RANK() OVER (ORDER BY element DESC NULLS LAST) AS element_rank_2
    FROM flattened
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, element_rank_2 - element_rank AS element_rank_2_minus_element_rank
    FROM metrics
    WHERE element_rank_2 <> 6
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        element,
        element_rank,
        element_rank_2,
        element_rank_2_minus_element_rank,
        RANK() OVER (ORDER BY element DESC NULLS LAST) AS element_rank_3,
        DENSE_RANK() OVER (ORDER BY element DESC) AS element_dense_rank
    FROM filtered
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY element_rank NULLS FIRST) AS element_rank_cume_dist,
        RANK() OVER (ORDER BY element_rank_2 NULLS FIRST) AS element_rank_2_rank,
        CUME_DIST() OVER (ORDER BY element_rank_2 DESC NULLS FIRST) AS element_rank_2_cume_dist
    FROM with_windows
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, element_rank_2_minus_element_rank - element_rank_2_cume_dist AS element_rank_2_minus_element_rank_minus_element_rank_2_cume_dist
    FROM enriched
    WHERE element IS NULL
)
SELECT
    id,
    element,
    element_rank,
    element_rank_2,
    element_rank_2_minus_element_rank,
    element_rank_3,
    element_dense_rank,
    element_rank_cume_dist,
    element_rank_2_rank
FROM valid
ORDER BY element_rank
