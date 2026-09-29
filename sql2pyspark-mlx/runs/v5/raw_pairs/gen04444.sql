SELECT
    id,
    effective_from,
    tier_limits,
    tier_prices,
    discounts,
    ARRAY_MIN(tier_prices) AS lowest_price,
    ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
    ELEMENT_AT(tier_prices, 1) AS entry_price,
    AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
FROM price_tiers
WHERE MONTH(effective_from) IN (2, 7, 11)
