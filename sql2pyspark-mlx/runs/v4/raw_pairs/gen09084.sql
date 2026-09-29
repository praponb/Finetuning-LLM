SELECT city
FROM stores
WHERE city = 'Berlin'
UNION
SELECT location
FROM departments
WHERE location IS NOT NULL;
