SELECT
    id,
    plan,
    effective_from,
    currency,
    EXPLODE(tier_limits) AS tier_value
FROM price_tiers
