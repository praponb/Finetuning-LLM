WITH unpacked AS (
    SELECT
        id,
        tier_prices,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
SELECT
    CASE WHEN entry_price >= 33 THEN 'PREMIUM' ELSE 'STANDARD' END AS entry_price_tier,
    MIN(avg_tier_price) AS min_avg_tier_price,
    APPROX_PERCENTILE(entry_price, 0.95) AS entry_price_p95,
    ROUND(AVG(entry_price), 2) AS avg_entry_price
FROM unpacked
GROUP BY CASE WHEN entry_price >= 33 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY entry_price_p95 DESC NULLS LAST
