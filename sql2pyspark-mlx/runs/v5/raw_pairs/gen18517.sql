SELECT o.id, o.status, o.discount
FROM orders o
WHERE o.discount >= 0.18 AND o.customer_id IN (SELECT id FROM customers WHERE is_active);
