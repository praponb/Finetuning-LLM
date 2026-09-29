SELECT
    id,
    effective_from,
    tier_limits,
    tier_prices,
    discounts,
    AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
FROM price_tiers
WHERE effective_from > '2025-09-01';
