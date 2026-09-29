SELECT id, customer_id, discount FROM orders WHERE status NOT IN ('PENDING', 'SHIPPED') ORDER BY 2, discount DESC, id LIMIT 20;
