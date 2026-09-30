SELECT * FROM (SELECT CASE WHEN budget >= 3549000 THEN 'high' WHEN budget >= 984000 THEN 'mid' ELSE 'low' END AS budget_bucket, dept_name, budget FROM departments)
PIVOT (COUNT(budget) FOR dept_name IN ('Engineering', 'Sales'))
ORDER BY budget_bucket
