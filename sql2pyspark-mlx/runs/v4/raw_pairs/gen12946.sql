SELECT
    id,
    plan,
    effective_from,
    element
FROM price_tiers
LATERAL VIEW EXPLODE(tier_prices) x AS element
