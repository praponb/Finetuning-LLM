SELECT
    CASE WHEN budget >= 3281000 THEN 'high' WHEN budget >= 1938000 THEN 'mid' ELSE 'low' END budget_bucket,
    location,
    ROUND(AVG(budget), 2) avg_budget,
    MAX(budget) - MIN(budget) budget_range
FROM departments
WHERE budget > 3949000
GROUP BY budget_bucket, location
