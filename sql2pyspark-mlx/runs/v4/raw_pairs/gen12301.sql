SELECT *
FROM (
    SELECT CASE WHEN budget >= 4162000 THEN 'high' WHEN budget >= 1964000 THEN 'mid' ELSE 'low' END AS budget_bucket, ROUND(STDDEV(budget), 2) AS stddev_budget
    FROM departments
    GROUP BY CASE WHEN budget >= 4162000 THEN 'high' WHEN budget >= 1964000 THEN 'mid' ELSE 'low' END
) AS x
WHERE stddev_budget >= 21
