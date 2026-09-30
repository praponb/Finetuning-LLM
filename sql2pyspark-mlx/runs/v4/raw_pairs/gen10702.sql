WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        plan,
        effective_from,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
)
-- Grouped metrics
SELECT
    plan,
    ROUND(AVG(lowest_price), 2) AS avg_lowest_price,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    COUNT(*) FILTER (WHERE plan = 'starter') AS starter_rows,
    ROUND(SUM(lowest_price) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS lowest_price_per_enterprise
FROM flattened_attrs
GROUP BY plan
HAVING avg_lowest_price < 0;
