SELECT id, order_date, discount
FROM orders
WHERE order_date >= (SELECT MAX(order_date) FROM orders WHERE order_date > '2025-07-09')
