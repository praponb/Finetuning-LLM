SELECT order_id, SUM(quantity) AS total_quantity, COUNT(DISTINCT quantity) AS unique_orders, MAX(quantity) AS max_quantity FROM order_items GROUP BY order_id HAVING unique_orders > 5;
