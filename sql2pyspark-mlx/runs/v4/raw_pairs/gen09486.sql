SELECT CASE WHEN budget >= 3756000 THEN 'high' WHEN budget >= 1409000 THEN 'mid' ELSE 'low' END AS budget_bucket, location, ROUND(AVG(budget), 2) AS avg_budget
FROM departments
WHERE budget BETWEEN 1563000 AND 3826000
GROUP BY CASE WHEN budget >= 3756000 THEN 'high' WHEN budget >= 1409000 THEN 'mid' ELSE 'low' END, location
ORDER BY location ASC
