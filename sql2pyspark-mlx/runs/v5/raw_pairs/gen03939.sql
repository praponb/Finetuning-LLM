SELECT country_group, city, total_age
FROM (
    SELECT CASE WHEN country IN ('SG', 'JP') THEN 'core' ELSE 'other' END AS country_group, city, SUM(age) AS total_age
    FROM customers
    GROUP BY CASE WHEN country IN ('SG', 'JP') THEN 'core' ELSE 'other' END, city
) AS sub
WHERE total_age < 360
ORDER BY country_group DESC, total_age DESC;
