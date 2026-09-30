SELECT s.id, s.country, c.city
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
