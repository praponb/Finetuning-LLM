SELECT
    id,
    effective_from,
    currency,
    tier_prices,
    ELEMENT_AT(tier_prices, 1) AS entry_price,
    ARRAY_MIN(tier_prices) AS lowest_price,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
FROM price_tiers
