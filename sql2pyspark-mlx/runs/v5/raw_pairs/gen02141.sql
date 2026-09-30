SELECT
    CASE WHEN dept_name IN ('HR', 'Legal') THEN 'priority' ELSE 'other' END AS dept_name_group,
    CASE WHEN budget >= 3428000 THEN 'high' WHEN budget >= 1760000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    dept_name,
    SUM(budget) AS total_budget,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM departments
WHERE budget <= 1476000
GROUP BY 1, 2, 3
HAVING COUNT(DISTINCT budget) > 4
ORDER BY total_budget DESC;
