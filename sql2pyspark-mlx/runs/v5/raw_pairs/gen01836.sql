SELECT CASE WHEN budget >= 3888000 THEN 'high' WHEN budget >= 1669000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(CASE WHEN location = 'New York' THEN budget ELSE 0 END) AS new_york_budget
FROM departments
WHERE dept_name NOT IN ('HR', 'Finance') AND dept_name IN ('Finance', 'Legal', 'Sales')
GROUP BY CASE WHEN budget >= 3888000 THEN 'high' WHEN budget >= 1669000 THEN 'mid' ELSE 'low' END
ORDER BY new_york_budget DESC;
