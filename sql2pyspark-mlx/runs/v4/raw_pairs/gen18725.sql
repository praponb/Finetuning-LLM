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
SELECT
    id,
    plan,
    currency,
    tier_value
FROM flattened
ORDER BY tier_value DESC
