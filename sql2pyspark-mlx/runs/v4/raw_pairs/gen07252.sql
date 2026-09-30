SELECT CASE WHEN budget >= 3938000 THEN 'high' WHEN budget >= 1005000 THEN 'mid' ELSE 'low' END AS budget_bucket, location, MAX(budget) AS max_budget
FROM departments
GROUP BY CASE WHEN budget >= 3938000 THEN 'high' WHEN budget >= 1005000 THEN 'mid' ELSE 'low' END, location
HAVING MAX(budget) < 1571000
ORDER BY budget_bucket, location DESC
