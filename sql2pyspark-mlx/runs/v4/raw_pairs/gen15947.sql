SELECT
    CASE WHEN budget >= 3040000 THEN 'high' WHEN budget >= 1139000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    SUM(budget) AS total_budget,
    COUNT(*) AS cnt,
    SUM(SUM(budget)) OVER (ORDER BY CASE WHEN budget >= 3040000 THEN 'high' WHEN budget >= 1139000 THEN 'mid' ELSE 'low' END) AS cumulative_budget
FROM departments
GROUP BY CASE WHEN budget >= 3040000 THEN 'high' WHEN budget >= 1139000 THEN 'mid' ELSE 'low' END
ORDER BY 2 DESC;
