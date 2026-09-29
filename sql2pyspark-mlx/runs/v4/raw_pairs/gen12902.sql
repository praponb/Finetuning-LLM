SELECT
    CASE WHEN budget >= 4338000 THEN 'high' WHEN budget >= 1866000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    dept_name,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) AS dept_name_values,
    SUM(budget) AS total_budget
FROM departments
WHERE location IN ('New York', 'London', 'Tokyo') AND dept_name = 'Sales'
GROUP BY CASE WHEN budget >= 4338000 THEN 'high' WHEN budget >= 1866000 THEN 'mid' ELSE 'low' END, dept_name
HAVING SUM(budget) > 10076000
ORDER BY dept_name_values DESC, total_budget, budget_bucket, dept_name
LIMIT 5
