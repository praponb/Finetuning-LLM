SELECT CASE WHEN budget >= 4280000 THEN 'high' WHEN budget >= 1862000 THEN 'mid' ELSE 'low' END AS budget_bucket, ROUND(STDDEV(budget), 2) AS stddev_budget
FROM departments
WHERE budget IS NOT NULL AND dept_name = 'Marketing' AND location = 'Chiang Mai'
GROUP BY 1;
