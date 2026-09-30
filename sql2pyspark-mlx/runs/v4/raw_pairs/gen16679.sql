SELECT
    id,
    currency,
    SLICE(tier_limits, 1, 2) AS first_two_limits,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
FROM price_tiers
