SELECT location, CASE WHEN budget >= 3267000 THEN 'high' WHEN budget >= 606000 THEN 'mid' ELSE 'low' END AS budget_bucket, MIN(budget) AS min_budget
FROM departments
GROUP BY location, CASE WHEN budget >= 3267000 THEN 'high' WHEN budget >= 606000 THEN 'mid' ELSE 'low' END
ORDER BY location;
