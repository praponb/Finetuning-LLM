WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) t AS element
)
-- Running and moving aggregates
SELECT
    id,
    effective_from,
    MIN(element) OVER (PARTITION BY currency) AS currency_min_element
FROM lines
