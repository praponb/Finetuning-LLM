SELECT
    id,
    effective_from,
    EXPLODE(tier_prices) AS element
FROM price_tiers
