SELECT country
FROM suppliers
WHERE city IN ('London', 'New York', 'Tokyo')
UNION
SELECT country
FROM sessions
WHERE device IN ('tablet', 'desktop')
ORDER BY country DESC;
