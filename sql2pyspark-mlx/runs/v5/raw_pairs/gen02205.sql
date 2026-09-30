SELECT customer_id, channel
FROM orders
EXCEPT DISTINCT
SELECT customer_id, channel
FROM orders
WHERE store_id IN (18, 36)
