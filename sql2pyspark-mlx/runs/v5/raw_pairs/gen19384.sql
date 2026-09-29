SELECT
    id,
    plan,
    effective_from,
    tier_limits,
    discounts,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
    AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
    SLICE(tier_limits, 1, 2) AS first_two_limits
FROM price_tiers
WHERE plan IN ('pro', 'enterprise')
