SELECT
    id,
    plan,
    effective_from,
    currency,
    tier_limits,
    tier_prices,
    discounts,
    ELEMENT_AT(tier_prices, 1) AS entry_price,
    AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
    ARRAY_MIN(tier_prices) AS lowest_price,
    ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
FROM price_tiers
WHERE plan IN ('enterprise', 'pro');
