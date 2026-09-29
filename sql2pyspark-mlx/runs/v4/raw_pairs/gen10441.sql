SELECT CASE WHEN city IN ('London', 'Bangkok') THEN 'group_a' ELSE 'other' END AS city_group, SUM(age) AS total_age
FROM customers
WHERE is_active = TRUE
GROUP BY 1;
