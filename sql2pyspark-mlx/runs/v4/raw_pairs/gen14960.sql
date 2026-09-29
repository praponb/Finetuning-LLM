SELECT
    id,
    plan,
    currency,
    tier_value
FROM price_tiers
LATERAL VIEW OUTER EXPLODE(tier_limits) e AS tier_value
