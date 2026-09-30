SELECT
    id,
    plan,
    currency,
    EXPLODE(tier_limits) AS element
FROM price_tiers
