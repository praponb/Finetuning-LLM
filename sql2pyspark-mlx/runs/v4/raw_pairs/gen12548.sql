SELECT c.credit_limit, c.is_active, s.device
FROM customers c
INNER JOIN sessions s ON s.customer_id = c.id
WHERE s.customer_id IS NULL AND s.device = 'mobile'
