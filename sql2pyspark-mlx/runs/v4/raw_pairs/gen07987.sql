WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(discounts) AS element
    FROM price_tiers
)
-- Multi-level aggregation
SELECT
    CASE WHEN element >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END AS element_tier,
    MAX(element) FILTER (WHERE plan = 'basic') AS basic_max_element,
    COUNT_IF(plan = 'pro') AS pro_events,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list_2,
    MAX(element) FILTER (WHERE plan = 'starter') AS starter_max_element,
    ROUND(AVG(element), 2) AS avg_element
FROM flattened
GROUP BY 1
ORDER BY pro_events;
