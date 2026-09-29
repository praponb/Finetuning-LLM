SELECT CASE WHEN budget >= 3952000 THEN 'high' WHEN budget >= 1614000 THEN 'mid' ELSE 'low' END AS budget_bucket, dept_name, SUM(CASE WHEN dept_name = 'Marketing' THEN budget ELSE 0 END) AS marketing_budget
FROM departments
WHERE dept_name <> 'Legal' AND budget > 3740000
GROUP BY budget_bucket, dept_name
