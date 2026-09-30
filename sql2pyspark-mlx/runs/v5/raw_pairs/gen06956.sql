SELECT s.id, s.device
FROM sessions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE credit_limit <= 13300)
