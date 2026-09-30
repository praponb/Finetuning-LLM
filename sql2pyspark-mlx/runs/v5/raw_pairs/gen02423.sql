WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS element
)
-- Running and moving aggregates
SELECT
    id,
    effective_from,
    CUME_DIST() OVER (ORDER BY element) AS element_cume_dist,
    RANK() OVER (ORDER BY element NULLS FIRST) AS element_rank
FROM flattened
