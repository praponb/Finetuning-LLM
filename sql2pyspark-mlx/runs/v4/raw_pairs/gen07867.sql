SELECT city, avg_age
FROM (
    SELECT city, AVG(age) avg_age
    FROM customers
    WHERE segment = 'Consumer'
    GROUP BY city
) x
WHERE avg_age > 46
ORDER BY avg_age DESC, city
