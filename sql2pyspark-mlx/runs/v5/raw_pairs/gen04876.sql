SELECT
    id,
    plan,
    effective_from,
    discounts,
    SLICE(tier_limits, 1, 2) AS first_two_limits,
    ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
FROM price_tiers;
