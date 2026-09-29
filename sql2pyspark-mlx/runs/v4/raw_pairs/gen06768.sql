SELECT launch_date FROM products UNION SELECT order_date AS launch_date FROM orders ORDER BY launch_date DESC;
