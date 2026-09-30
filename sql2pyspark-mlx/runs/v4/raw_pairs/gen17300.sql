WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        plan,
        tier_limits,
        ARRAY_MIN(tier_prices) AS lowest_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    plan,
    CASE WHEN lowest_price >= 36 THEN 'HIGH' ELSE 'LOW' END AS lowest_price_tier,
    SUM(CASE WHEN plan = 'starter' THEN entry_price ELSE 0 END) AS starter_entry_price,
    SUM(CASE WHEN plan = 'enterprise' THEN lowest_price ELSE 0 END) AS enterprise_lowest_price,
    MAX(entry_price) AS max_entry_price
FROM flattened_attrs
GROUP BY 1, 2
ORDER BY starter_entry_price DESC
