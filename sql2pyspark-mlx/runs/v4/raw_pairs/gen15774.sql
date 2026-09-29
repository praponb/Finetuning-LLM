SELECT
    id,
    location,
    ROUND(budget * 1.15, 2) AS budget_adjusted,
    CASE WHEN dept_name = 'Marketing' THEN 1 ELSE 0 END AS is_marketing
FROM departments
WHERE budget >= 3967000 AND location IN ('Chiang Mai', 'Tokyo', 'Singapore')
ORDER BY id;
