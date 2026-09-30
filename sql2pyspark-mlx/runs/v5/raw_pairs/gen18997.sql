WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        plan,
        currency,
        tier_limits,
        tier_prices,
        discounts,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
)
-- Multi-level aggregation
SELECT
    CASE WHEN avg_tier_price >= 32 THEN 'HIGH' ELSE 'LOW' END AS avg_tier_price_tier,
    currency,
    CASE WHEN lowest_price >= 31 THEN 'PREMIUM' ELSE 'STANDARD' END AS lowest_price_tier,
    SUM(avg_tier_price) FILTER (WHERE currency = 'EUR') AS eur_total_avg_tier_price,
    SUM(CASE WHEN currency = 'THB' THEN avg_tier_price ELSE 0 END) AS thb_avg_tier_price,
    ROUND(SUM(avg_tier_price) / NULLIF(COUNT(CASE WHEN currency = 'THB' THEN 1 END), 0), 2) AS avg_tier_price_per_thb
FROM unpacked
GROUP BY GROUPING SETS ((CASE WHEN avg_tier_price >= 32 THEN 'HIGH' ELSE 'LOW' END, currency, CASE WHEN lowest_price >= 31 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN avg_tier_price >= 32 THEN 'HIGH' ELSE 'LOW' END, currency), (CASE WHEN avg_tier_price >= 32 THEN 'HIGH' ELSE 'LOW' END))
ORDER BY currency
