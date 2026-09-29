SELECT dept_name, CASE WHEN budget >= 4476000 THEN 'high' WHEN budget >= 1964000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(*) AS cnt
FROM departments
GROUP BY dept_name, budget_bucket
ORDER BY budget_bucket DESC;
