SELECT customer_id, store_id
FROM orders
WHERE status = 'PENDING'
UNION
SELECT customer_id, store_id
FROM orders
WHERE channel = 'mobile';
