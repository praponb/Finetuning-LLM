SELECT
    id,
    plan,
    tier_prices,
    discounts,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
    SLICE(tier_limits, 1, 2) AS first_two_limits
FROM price_tiers;
