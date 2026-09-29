SELECT
    id,
    plan,
    effective_from,
    tier_limits,
    tier_prices,
    discounts,
    ARRAY_MIN(tier_prices) AS lowest_price,
    ELEMENT_AT(tier_prices, 1) AS entry_price,
    ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
FROM price_tiers
WHERE plan IN ('basic', 'enterprise', 'pro')
