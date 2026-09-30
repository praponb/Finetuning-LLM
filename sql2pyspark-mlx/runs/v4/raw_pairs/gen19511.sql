WITH flattened_attrs AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_limits,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
    WHERE effective_from <= '2025-02-01'
)
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(avg_tier_price, ARRAY(0.25, 0.5, 0.75)) AS avg_tier_price_quartiles,
    AVG(avg_tier_price) FILTER (WHERE currency = 'EUR') AS eur_avg_avg_tier_price,
    ROUND(AVG(avg_tier_price), 2) AS avg_avg_tier_price,
    ROUND(SUM(avg_tier_price) / NULLIF(COUNT(CASE WHEN currency = 'THB' THEN 1 END), 0), 2) AS avg_tier_price_per_thb
FROM flattened_attrs
GROUP BY CUBE (DATE_TRUNC('WEEK', effective_from), DATE_TRUNC('YEAR', effective_from))
ORDER BY year_start ASC
