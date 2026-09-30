WITH unpacked AS (
    SELECT
        id,
        tier_prices,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ARRAY_MIN(tier_prices) AS lowest_price,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
    FROM price_tiers
)
SELECT
    CASE WHEN avg_tier_price >= 27 THEN 'HIGH' ELSE 'LOW' END AS avg_tier_price_tier,
    CASE WHEN lowest_price >= 30 THEN 'PREMIUM' ELSE 'STANDARD' END AS lowest_price_tier,
    ROUND(AVG(avg_tier_price), 2) AS avg_avg_tier_price,
    MIN(lowest_price) AS min_lowest_price,
    APPROX_PERCENTILE(avg_tier_price, 0.9) AS avg_tier_price_p90,
    MAX(lowest_price) AS max_lowest_price,
    PERCENTILE_APPROX(avg_tier_price, 0.9) AS avg_tier_price_p90_2,
    MAX(avg_tier_price) AS max_avg_tier_price,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN avg_tier_price >= 27 THEN 'HIGH' ELSE 'LOW' END ORDER BY APPROX_PERCENTILE(avg_tier_price, 0.9) DESC) AS avg_tier_price_p90_percent_rank
FROM unpacked
GROUP BY avg_tier_price_tier, lowest_price_tier
HAVING min_lowest_price >= 0;
