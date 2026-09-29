SELECT cost FROM products EXCEPT SELECT amount FROM orders WHERE order_date < '2024-10-01'
