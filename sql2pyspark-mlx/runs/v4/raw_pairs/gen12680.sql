SELECT
    CASE WHEN budget >= 4128000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    ROUND(AVG(budget), 2) AS avg_budget,
    MAX(budget) AS max_budget
FROM departments
GROUP BY CASE WHEN budget >= 4128000 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY avg_budget DESC NULLS LAST
