SELECT c.id, c.segment, c.name, c.credit_limit FROM customers c WHERE EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id AND s.device IN ('mobile', 'desktop'))
