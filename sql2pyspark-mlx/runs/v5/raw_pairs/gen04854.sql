SELECT CASE WHEN budget >= 4056000 THEN 'high' WHEN budget >= 1036000 THEN 'mid' ELSE 'low' END AS budget_bucket, location, SUM(CASE WHEN location = 'Bangkok' THEN budget ELSE 0 END) AS bangkok_budget
FROM departments
WHERE location NOT IN ('London', 'Chiang Mai', 'Singapore')
GROUP BY CASE WHEN budget >= 4056000 THEN 'high' WHEN budget >= 1036000 THEN 'mid' ELSE 'low' END, location
ORDER BY location
