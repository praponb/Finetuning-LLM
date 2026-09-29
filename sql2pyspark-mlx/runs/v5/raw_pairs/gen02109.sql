SELECT
    id,
    currency,
    tier_limits,
    tier_prices,
    discounts,
    AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
    ARRAY_MIN(tier_prices) AS lowest_price,
    ELEMENT_AT(tier_prices, 1) AS entry_price
FROM price_tiers
