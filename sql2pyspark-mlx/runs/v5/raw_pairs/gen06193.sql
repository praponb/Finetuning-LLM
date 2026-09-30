SELECT s.id, s.customer_id, s.device FROM sessions s WHERE s.browser IN ('Safari', 'Chrome') AND s.customer_id IN (SELECT id FROM customers WHERE age <= 39)
