SELECT order_date, customer_id
FROM orders
WHERE customer_id = 5
UNION
SELECT order_date, customer_id
FROM orders
WHERE MONTH(order_date) IN (6, 9, 12)
ORDER BY order_date DESC;
