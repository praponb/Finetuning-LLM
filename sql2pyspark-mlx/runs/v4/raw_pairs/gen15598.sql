WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        effective_from,
        currency,
        discounts,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
    WHERE effective_from >= '2024-11-01'
)
-- Multi-level aggregation
SELECT
    CASE WHEN avg_tier_price >= 30 THEN 'LARGE' ELSE 'SMALL' END AS avg_tier_price_tier,
    AVG(avg_tier_price) AS avg_avg_tier_price,
    COUNT(*) AS event_count
FROM parsed
GROUP BY 1
