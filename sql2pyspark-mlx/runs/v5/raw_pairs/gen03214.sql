WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS tier_value
)
-- Ranking and gap features
SELECT
    id,
    plan,
    tier_value,
    CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value NULLS FIRST) AS tier_value_cume_dist
FROM flattened
