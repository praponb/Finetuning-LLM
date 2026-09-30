SELECT
    id,
    plan,
    effective_from,
    currency,
    EXPLODE(tier_prices) AS tier_value
FROM price_tiers
