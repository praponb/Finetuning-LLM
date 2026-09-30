SELECT c.id, c.name, c.is_active
FROM customers c
WHERE NOT c.is_active AND EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id)
ORDER BY id
LIMIT 10;
