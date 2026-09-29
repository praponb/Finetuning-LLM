SELECT s.id, s.device
FROM sessions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE age = 53)
ORDER BY s.id DESC, device
