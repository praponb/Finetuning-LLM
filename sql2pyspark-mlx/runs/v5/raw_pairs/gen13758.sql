SELECT
    location,
    CASE WHEN budget >= 3922000 THEN 'high' WHEN budget >= 1252000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    ROUND(AVG(budget), 2) AS avg_budget,
    SUM(CASE WHEN dept_name = 'Legal' THEN 1 ELSE 0 END) AS legal_count,
    ROUND(STDDEV(budget), 2) AS stddev_budget
FROM departments
WHERE location NOT IN ('Singapore', 'London', 'Chiang Mai') AND location <> 'Chiang Mai'
GROUP BY location, budget_bucket
HAVING SUM(CASE WHEN dept_name = 'Legal' THEN 1 ELSE 0 END) > 29
ORDER BY avg_budget DESC, location, budget_bucket
LIMIT 3
