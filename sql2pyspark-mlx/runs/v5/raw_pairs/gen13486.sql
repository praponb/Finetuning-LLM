WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        currency,
        tier_limits,
        tier_prices,
        discounts,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    currency,
    CASE WHEN lowest_price >= 30 THEN 'LARGE' ELSE 'SMALL' END AS lowest_price_tier,
    PERCENTILE_APPROX(avg_tier_price, 0.5) AS avg_tier_price_p50,
    COUNT(CASE WHEN currency = 'EUR' THEN 1 END) AS eur_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM flattened_attrs
GROUP BY ROLLUP (currency, CASE WHEN lowest_price >= 30 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY eur_count DESC NULLS LAST
