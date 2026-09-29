WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        plan,
        tier_limits,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
    WHERE plan = 'starter'
)
-- Multi-level aggregation
SELECT
    CASE WHEN avg_tier_price >= 32 THEN 'HIGH' ELSE 'LOW' END AS avg_tier_price_tier,
    SUM(avg_tier_price) FILTER (WHERE plan = 'enterprise') AS enterprise_total_avg_tier_price,
    ROUND(AVG(avg_tier_price), 2) AS avg_avg_tier_price,
    COUNT(CASE WHEN plan = 'starter' THEN 1 END) AS starter_count
FROM flattened_attrs
GROUP BY CASE WHEN avg_tier_price >= 32 THEN 'HIGH' ELSE 'LOW' END
ORDER BY starter_count DESC
