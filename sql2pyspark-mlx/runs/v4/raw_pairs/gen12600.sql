SELECT status, unique_statuss
FROM (
    SELECT status, COUNT(DISTINCT amount) AS unique_statuss
    FROM orders
    GROUP BY status
) AS s
WHERE unique_statuss >= 7
ORDER BY status, unique_statuss DESC
LIMIT 3
