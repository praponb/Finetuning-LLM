SELECT city
FROM stores
WHERE region <> 'Central'
UNION ALL
SELECT city
FROM stores
WHERE city = 'London'
