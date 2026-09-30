SELECT order_id, product_id, COUNT(*) AS total_count FROM order_items WHERE product_id = 7 GROUP BY order_id, product_id HAVING total_count > 2
