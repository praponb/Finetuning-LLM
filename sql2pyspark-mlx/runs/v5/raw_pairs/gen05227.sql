SELECT CASE WHEN budget >= 4503000 THEN 'high' WHEN budget >= 2030000 THEN 'mid' ELSE 'low' END AS budget_bucket, MAX(budget) - MIN(budget) AS budget_range, ROUND(STDDEV(budget), 2) AS stddev_budget
FROM departments
WHERE budget > 2900000 AND (dept_name IN ('Engineering', 'Sales', 'Legal') OR location IN ('Singapore', 'London'))
GROUP BY CASE WHEN budget >= 4503000 THEN 'high' WHEN budget >= 2030000 THEN 'mid' ELSE 'low' END
ORDER BY stddev_budget, budget_bucket
LIMIT 1;
