WITH unpacked AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_limits,
        tier_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
    WHERE effective_from < '2024-02-01'
)
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    CASE WHEN entry_price >= 34 THEN 'HIGH' ELSE 'LOW' END AS entry_price_tier,
    MAX(entry_price) FILTER (WHERE currency = 'USD') AS usd_max_entry_price,
    PERCENTILE_APPROX(entry_price, ARRAY(0.25, 0.5, 0.75)) AS entry_price_quartiles
FROM unpacked
GROUP BY DATE_TRUNC('WEEK', effective_from), CASE WHEN entry_price >= 34 THEN 'HIGH' ELSE 'LOW' END
HAVING MAX(entry_price) FILTER (WHERE currency = 'USD') >= 0
ORDER BY week_start NULLS FIRST;
