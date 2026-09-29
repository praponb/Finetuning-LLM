SELECT id, plan, ARRAY_MIN(tier_prices) AS lowest_price, TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps FROM price_tiers;
