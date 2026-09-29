SELECT
    id,
    plan,
    currency,
    tier_prices,
    ELEMENT_AT(tier_prices, 1) AS entry_price,
    ARRAY_MIN(tier_prices) AS lowest_price,
    SLICE(tier_limits, 1, 2) AS first_two_limits,
    AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
    ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
FROM price_tiers
