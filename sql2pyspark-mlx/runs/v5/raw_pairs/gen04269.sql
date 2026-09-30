SELECT
    dept_name,
    CASE WHEN budget >= 3817000 THEN 'high' WHEN budget >= 634000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END) AS engineering_count,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list
FROM departments
GROUP BY dept_name, CASE WHEN budget >= 3817000 THEN 'high' WHEN budget >= 634000 THEN 'mid' ELSE 'low' END
ORDER BY 2;
