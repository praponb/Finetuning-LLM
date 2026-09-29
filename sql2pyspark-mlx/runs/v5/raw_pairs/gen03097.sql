SELECT c.id, c.email, COUNT(s.id) AS session_count
FROM customers c
JOIN sessions s ON c.id = s.customer_id
WHERE s.customer_id IS NOT NULL
GROUP BY c.id, c.email
HAVING COUNT(s.id) > 3
