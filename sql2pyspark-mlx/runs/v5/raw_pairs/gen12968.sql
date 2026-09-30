WITH flattened_attrs AS (
    SELECT
        id,
        plan,
        tier_prices,
        discounts,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
)
SELECT
    CASE WHEN lowest_price >= 26 THEN 'PREMIUM' ELSE 'STANDARD' END AS lowest_price_tier,
    COUNT_IF(plan = 'pro') AS pro_events,
    COUNT_IF(plan = 'enterprise') AS enterprise_events,
    ROUND(AVG(avg_tier_price), 2) AS avg_avg_tier_price
FROM flattened_attrs
GROUP BY CUBE (CASE WHEN lowest_price >= 26 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING COUNT_IF(plan = 'pro') > 1
ORDER BY enterprise_events NULLS FIRST;
