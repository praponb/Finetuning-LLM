SELECT country
FROM customers
UNION
SELECT country
FROM sessions
ORDER BY country;
