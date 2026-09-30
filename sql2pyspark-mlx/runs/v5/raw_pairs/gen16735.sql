SELECT
    id,
    plan,
    effective_from,
    currency,
    tier_prices,
    discounts,
    SLICE(tier_limits, 1, 2) AS first_two_limits,
    ARRAY_MIN(tier_prices) AS lowest_price,
    ELEMENT_AT(tier_prices, 1) AS entry_price,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
    ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
FROM price_tiers
