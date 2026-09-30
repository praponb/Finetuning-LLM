SELECT signup_date
FROM customers
UNION
SELECT order_date
FROM orders
WHERE store_id IN (3, 12, 36, 37)
ORDER BY signup_date
