SELECT s.device, c.email, c.is_active
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
