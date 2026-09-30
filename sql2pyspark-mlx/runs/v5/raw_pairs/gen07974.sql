WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_limits,
        discounts,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
    WHERE currency = 'EUR'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN avg_tier_price >= 25 THEN 'LARGE' ELSE 'SMALL' END AS avg_tier_price_tier,
    ROUND(SUM(avg_tier_price) / NULLIF(COUNT(CASE WHEN currency = 'EUR' THEN 1 END), 0), 2) AS avg_tier_price_per_eur,
    SUM(CASE WHEN plan = 'pro' THEN avg_tier_price ELSE 0 END) AS pro_avg_tier_price,
    ROUND(AVG(lowest_price), 2) AS avg_lowest_price
FROM unpacked
GROUP BY CUBE (CASE WHEN avg_tier_price >= 25 THEN 'LARGE' ELSE 'SMALL' END);
