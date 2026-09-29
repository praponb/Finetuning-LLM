SELECT id, customer_id, store_id
FROM orders
WHERE discount > 0.31
UNION ALL
SELECT id, customer_id, store_id
FROM orders
WHERE status IN ('PENDING', 'SHIPPED');
