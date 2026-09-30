SELECT o.id, o.order_date
FROM orders o
WHERE o.customer_id NOT IN (SELECT id FROM customers WHERE YEAR(signup_date) >= 2023)
ORDER BY order_date DESC
