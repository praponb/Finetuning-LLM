SELECT
    id,
    plan,
    effective_from,
    currency,
    EXPLODE(tier_limits) AS element
FROM price_tiers;
