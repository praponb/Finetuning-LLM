SELECT CASE WHEN budget >= 3069000 THEN 'high' WHEN budget >= 1297000 THEN 'mid' ELSE 'low' END AS budget_bucket, MAX(budget) - MIN(budget) AS budget_range
FROM departments
WHERE dept_name IS NOT NULL AND dept_name = 'HR'
GROUP BY CASE WHEN budget >= 3069000 THEN 'high' WHEN budget >= 1297000 THEN 'mid' ELSE 'low' END;
