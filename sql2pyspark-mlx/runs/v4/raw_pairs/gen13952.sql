SELECT country
FROM customers
WHERE credit_limit <= 11000
INTERSECT
SELECT country
FROM sessions
ORDER BY country DESC;
