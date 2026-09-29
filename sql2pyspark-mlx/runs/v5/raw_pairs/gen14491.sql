SELECT
    id,
    discounts,
    ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
    AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
    SLICE(tier_limits, 1, 2) AS first_two_limits,
    ELEMENT_AT(tier_prices, 1) AS entry_price
FROM price_tiers
