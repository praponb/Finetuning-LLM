WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(tier_prices) AS element
    FROM price_tiers
)
SELECT
    plan,
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    APPROX_PERCENTILE(element, ARRAY(0.25, 0.5, 0.75)) AS element_quartiles,
    SUM(CASE WHEN plan = 'basic' THEN element ELSE 0 END) AS basic_element,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(element) FILTER (WHERE plan = 'enterprise') AS enterprise_total_element,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows
FROM lines
GROUP BY GROUPING SETS ((plan, DATE_TRUNC('MONTH', effective_from)), (plan))
