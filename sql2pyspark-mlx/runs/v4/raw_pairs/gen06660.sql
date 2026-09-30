SELECT customer_id, store_id
FROM orders
EXCEPT
SELECT customer_id, store_id
FROM orders
WHERE order_date < '2025-08-01';
