WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        plan,
        currency,
        tier_prices,
        discounts,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
    WHERE plan IN ('starter', 'enterprise', 'basic')
)
-- Grouped metrics
SELECT
    CASE WHEN avg_tier_price >= 30 THEN 'PREMIUM' ELSE 'STANDARD' END AS avg_tier_price_tier,
    SUM(CASE WHEN plan = 'starter' THEN avg_tier_price ELSE 0 END) AS starter_avg_tier_price,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list
FROM flattened_attrs
GROUP BY CASE WHEN avg_tier_price >= 30 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY starter_avg_tier_price DESC
