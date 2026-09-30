WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        currency,
        tier_prices,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
    WHERE currency IN ('THB', 'USD', 'EUR')
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN lowest_price >= 34 THEN 'HIGH' ELSE 'LOW' END AS lowest_price_tier,
    currency,
    PERCENTILE_APPROX(lowest_price, ARRAY(0.25, 0.5, 0.75)) AS lowest_price_quartiles,
    APPROX_PERCENTILE(lowest_price, 0.95) AS lowest_price_p95,
    COUNT(CASE WHEN currency = 'USD' THEN 1 END) AS usd_count,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN lowest_price >= 34 THEN 'HIGH' ELSE 'LOW' END ORDER BY APPROX_PERCENTILE(lowest_price, 0.95) DESC) AS lowest_price_p95_percent_rank
FROM unpacked
GROUP BY CASE WHEN lowest_price >= 34 THEN 'HIGH' ELSE 'LOW' END, currency
