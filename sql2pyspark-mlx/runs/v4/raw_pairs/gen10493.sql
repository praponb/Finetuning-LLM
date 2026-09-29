WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        effective_from,
        tier_limits,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ARRAY_MIN(tier_prices) AS lowest_price,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
    WHERE YEAR(effective_from) = 2025
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    SUM(entry_price) AS total_entry_price,
    AVG(lowest_price) AS avg_lowest_price,
    COUNT(*) AS row_count,
    SUM(CASE WHEN plan = 'starter' THEN entry_price ELSE 0 END) AS starter_entry_price,
    GROUPING_ID() AS grouping_id
FROM flattened_attrs
GROUP BY ROLLUP (DATE_TRUNC('MONTH', effective_from))
ORDER BY activity_month DESC
