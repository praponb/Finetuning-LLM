WITH unpacked AS (
    SELECT
        id,
        effective_from,
        tier_limits,
        discounts,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
    WHERE YEAR(effective_from) = 2024
)
SELECT
    CASE WHEN lowest_price >= 31 THEN 'PREMIUM' ELSE 'STANDARD' END AS lowest_price_tier,
    PERCENTILE_APPROX(avg_tier_price, ARRAY(0.25, 0.5, 0.75)) AS avg_tier_price_quartiles,
    COUNT(*) AS row_count,
    COUNT(*) AS event_count,
    ROUND(AVG(avg_tier_price), 2) AS avg_avg_tier_price
FROM unpacked
GROUP BY lowest_price_tier
HAVING avg_avg_tier_price > 0
ORDER BY lowest_price_tier ASC
