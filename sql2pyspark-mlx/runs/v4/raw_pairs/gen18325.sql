WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) x AS tier_value
)
SELECT
    CASE WHEN tier_value >= 76 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_tier,
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    currency,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    APPROX_PERCENTILE(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    COUNT(DISTINCT plan) AS distinct_plans,
    APPROX_PERCENTILE(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles_2,
    SUM(CASE WHEN currency = 'THB' THEN tier_value ELSE 0 END) AS thb_tier_value,
    GROUPING_ID() AS grouping_id
FROM flattened
GROUP BY ROLLUP (CASE WHEN tier_value >= 76 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', effective_from), currency)
ORDER BY tier_value_tier NULLS FIRST;
