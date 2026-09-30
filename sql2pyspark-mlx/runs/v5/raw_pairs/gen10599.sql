WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        effective_from,
        tier_limits,
        discounts,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN lowest_price >= 30 THEN 'LARGE' ELSE 'SMALL' END AS lowest_price_tier,
    AVG(lowest_price) AS avg_lowest_price,
    COUNT(DISTINCT id) AS distinct_ids
FROM unpacked
GROUP BY CASE WHEN lowest_price >= 30 THEN 'LARGE' ELSE 'SMALL' END
