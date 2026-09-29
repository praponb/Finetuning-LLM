WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    *,
    CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value NULLS FIRST) AS tier_value_cume_dist,
    CUME_DIST() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_cume_dist_2
FROM exploded
