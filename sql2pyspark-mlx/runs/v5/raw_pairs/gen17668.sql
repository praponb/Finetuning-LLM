SELECT *
FROM (
    SELECT is_active, COUNT(age) AS age_count, ROUND(STDDEV(age), 2) AS stddev_age
    FROM customers
    GROUP BY is_active
) AS s
WHERE age_count < 7
ORDER BY age_count ASC;
