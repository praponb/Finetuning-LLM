SELECT CASE WHEN budget >= 3206000 THEN 'high' WHEN budget >= 1938000 THEN 'mid' ELSE 'low' END budget_bucket, SUM(budget) total_budget, SUM(SUM(budget)) OVER (ORDER BY CASE WHEN budget >= 3206000 THEN 'high' WHEN budget >= 1938000 THEN 'mid' ELSE 'low' END) AS cumulative_budget
FROM departments
WHERE location = 'Tokyo'
GROUP BY CASE WHEN budget >= 3206000 THEN 'high' WHEN budget >= 1938000 THEN 'mid' ELSE 'low' END
