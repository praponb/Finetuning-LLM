WITH parsed AS (
    SELECT
        id,
        effective_from,
        tier_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ARRAY_MIN(tier_prices) AS lowest_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
)
SELECT
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    APPROX_PERCENTILE(avg_tier_price, ARRAY(0.25, 0.5, 0.75)) AS avg_tier_price_quartiles,
    PERCENTILE_APPROX(lowest_price, 0.95) AS lowest_price_p95,
    PERCENTILE_APPROX(avg_tier_price, ARRAY(0.25, 0.5, 0.75)) AS avg_tier_price_quartiles_2,
    PERCENTILE_APPROX(lowest_price, 0.95) AS lowest_price_p95_2,
    ROUND(AVG(entry_price), 2) AS avg_entry_price,
    ROUND(AVG(avg_tier_price), 2) AS avg_avg_tier_price
FROM parsed
GROUP BY DATE_TRUNC('YEAR', effective_from)
ORDER BY avg_avg_tier_price DESC;
