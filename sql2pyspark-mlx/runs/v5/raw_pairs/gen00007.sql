SELECT
    id,
    plan,
    effective_from,
    tier_value
FROM price_tiers
LATERAL VIEW OUTER EXPLODE(tier_prices) t AS tier_value;
