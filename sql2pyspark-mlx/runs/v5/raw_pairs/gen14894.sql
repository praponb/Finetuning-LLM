SELECT country, MIN(age) AS min_age FROM customers WHERE city = 'Berlin' AND age <> 38 GROUP BY country HAVING MIN(age) > 32 ORDER BY country
