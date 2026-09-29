SELECT
    dept_name,
    CASE WHEN budget >= 3515000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    COUNT(*) AS row_count,
    AVG(budget) FILTER (WHERE dept_name = 'Engineering') AS engineering_avg_budget
FROM departments
GROUP BY ROLLUP (dept_name, CASE WHEN budget >= 3515000 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY budget_tier ASC NULLS LAST
