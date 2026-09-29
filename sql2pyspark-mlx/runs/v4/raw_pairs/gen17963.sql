SELECT CASE WHEN budget >= 4143000 THEN 'high' WHEN budget >= 1224000 THEN 'mid' ELSE 'low' END budget_bucket, ROUND(STDDEV(budget), 2) stddev_budget, MAX(budget) - MIN(budget) budget_range
FROM departments
GROUP BY 1
