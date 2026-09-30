WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_limits,
        discounts,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
    WHERE currency IN ('EUR', 'THB')
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN entry_price >= 26 THEN 'PREMIUM' ELSE 'STANDARD' END AS entry_price_tier,
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    COUNT(DISTINCT plan) AS distinct_plans,
    COUNT(*) FILTER (WHERE currency = 'THB') AS thb_rows,
    COUNT(*) AS row_count,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    ROUND(SUM(avg_tier_price) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS avg_tier_price_per_basic,
    AVG(avg_tier_price) AS avg_avg_tier_price,
    GROUPING(CASE WHEN entry_price >= 26 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
FROM flattened_attrs
GROUP BY CUBE (CASE WHEN entry_price >= 26 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('YEAR', effective_from))
ORDER BY num_currencies DESC NULLS LAST;
