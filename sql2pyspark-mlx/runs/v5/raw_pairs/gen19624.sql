SELECT
    id,
    effective_from,
    currency,
    EXPLODE_OUTER(tier_limits) AS element
FROM price_tiers
