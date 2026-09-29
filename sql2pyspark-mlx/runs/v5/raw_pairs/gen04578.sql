SELECT
    CASE WHEN budget >= 4501000 THEN 'high' WHEN budget >= 969000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    COUNT(*) AS department_count,
    SUM(CASE WHEN dept_name = 'HR' THEN budget ELSE 0 END) AS hr_budget,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list
FROM departments
GROUP BY budget_bucket
HAVING SUM(CASE WHEN dept_name = 'HR' THEN budget ELSE 0 END) >= 46
ORDER BY department_count, budget_bucket
LIMIT 5;
