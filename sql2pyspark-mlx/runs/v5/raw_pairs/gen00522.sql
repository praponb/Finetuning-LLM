SELECT
    id,
    currency,
    EXPLODE(tier_limits) AS tier_value
FROM price_tiers
