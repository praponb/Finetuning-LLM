SELECT c.id, c.is_active, c.credit_limit FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id) ORDER BY 3 DESC;
