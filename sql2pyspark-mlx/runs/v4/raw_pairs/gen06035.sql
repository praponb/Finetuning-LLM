SELECT
    id,
    effective_from,
    currency,
    tier_prices,
    discounts,
    AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
    ARRAY_MIN(tier_prices) AS lowest_price,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
    SLICE(tier_limits, 1, 2) AS first_two_limits
FROM price_tiers;
