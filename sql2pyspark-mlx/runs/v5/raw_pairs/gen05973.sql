SELECT
    id,
    plan,
    effective_from,
    element
FROM price_tiers
LATERAL VIEW EXPLODE(tier_limits) e AS element;
