SELECT CASE WHEN budget >= 3495000 THEN 'high' WHEN budget >= 1237000 THEN 'mid' ELSE 'low' END AS budget_bucket, MAX(budget) - MIN(budget) AS budget_range, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) AS dept_name_values
FROM departments
WHERE dept_name IN ('Legal', 'Finance') AND dept_name IN ('Sales', 'Engineering')
GROUP BY 1
ORDER BY budget_bucket, budget_range;
