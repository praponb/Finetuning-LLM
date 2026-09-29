SELECT
    CASE WHEN budget >= 4504000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    MAX(budget) FILTER (WHERE dept_name = 'Marketing') AS marketing_max_budget,
    APPROX_PERCENTILE(budget, 0.95) AS budget_p95,
    COUNT(*) AS cnt
FROM departments
GROUP BY 1
HAVING APPROX_PERCENTILE(budget, 0.95) < 0;
