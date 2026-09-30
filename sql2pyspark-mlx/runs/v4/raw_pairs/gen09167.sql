SELECT
    CASE WHEN budget >= 3407000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    COUNT(*) AS event_count,
    COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END) AS sales_count,
    COUNT(DISTINCT location) AS distinct_locations,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list
FROM departments
GROUP BY CASE WHEN budget >= 3407000 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING distinct_locations >= 1
