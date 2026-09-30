SELECT budget_bucket, budget_range, pct_engineering
FROM (
    SELECT CASE WHEN budget >= 4085000 THEN 'high' WHEN budget >= 1012000 THEN 'mid' ELSE 'low' END budget_bucket, MAX(budget) - MIN(budget) budget_range, ROUND((100 * SUM(CASE WHEN dept_name = 'Engineering' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_engineering
    FROM departments
    GROUP BY CASE WHEN budget >= 4085000 THEN 'high' WHEN budget >= 1012000 THEN 'mid' ELSE 'low' END
) x
WHERE pct_engineering >= 40
ORDER BY 3;
