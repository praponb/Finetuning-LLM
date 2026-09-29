SELECT customer_id, order_date
FROM orders
WHERE order_date >= '2023-09-01'
UNION ALL
SELECT customer_id, order_date
FROM orders
WHERE store_id IN (17, 28, 39)
ORDER BY customer_id
