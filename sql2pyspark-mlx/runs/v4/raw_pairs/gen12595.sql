SELECT city, MIN(age) AS min_age
FROM customers
WHERE segment NOT IN ('Consumer', 'Corporate')
GROUP BY city
HAVING MIN(age) > 49
ORDER BY city DESC;
