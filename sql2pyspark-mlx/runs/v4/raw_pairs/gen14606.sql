SELECT budget_bucket, total_budget, stddev_budget
FROM (
    SELECT CASE WHEN budget >= 3191000 THEN 'high' WHEN budget >= 1849000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(budget) AS total_budget, ROUND(STDDEV(budget), 2) AS stddev_budget
    FROM departments
    WHERE budget BETWEEN 1537000 AND 1692000
    GROUP BY CASE WHEN budget >= 3191000 THEN 'high' WHEN budget >= 1849000 THEN 'mid' ELSE 'low' END
) AS t
WHERE total_budget < 18574000
ORDER BY total_budget DESC
