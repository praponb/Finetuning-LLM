WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        effective_from,
        currency,
        tier_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
)
-- Grouped metrics
SELECT
    CASE WHEN entry_price >= 35 THEN 'PREMIUM' ELSE 'STANDARD' END AS entry_price_tier,
    COUNT(*) FILTER (WHERE currency = 'THB') AS thb_rows,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    COUNT(*) FILTER (WHERE currency = 'USD') AS usd_rows,
    GROUPING_ID() AS grouping_level
FROM flattened_attrs
GROUP BY ROLLUP (CASE WHEN entry_price >= 35 THEN 'PREMIUM' ELSE 'STANDARD' END);
