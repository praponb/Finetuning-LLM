SELECT c.id, c.email
FROM customers c
WHERE EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id AND s.browser IN ('Chrome', 'Firefox', 'Safari'))
