SELECT
    CASE WHEN budget >= 3360000 THEN 'high' WHEN budget >= 1170000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    dept_name,
    MIN(budget) AS min_budget,
    SUM(budget) AS total_budget,
    COUNT(*) AS row_count
FROM departments
WHERE location IN ('Berlin', 'Bangkok')
GROUP BY CASE WHEN budget >= 3360000 THEN 'high' WHEN budget >= 1170000 THEN 'mid' ELSE 'low' END, dept_name
HAVING total_budget < 7518000
