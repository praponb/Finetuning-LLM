SELECT order_id, product_id, COUNT(unit_price) AS unit_price_count, COUNT(*) AS num_order_items FROM order_items GROUP BY order_id, product_id HAVING COUNT(*) < 5 ORDER BY order_id DESC
