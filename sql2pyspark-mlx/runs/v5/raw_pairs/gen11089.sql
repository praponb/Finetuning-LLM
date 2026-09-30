SELECT product_id, order_id, COUNT(*) AS num_order_items FROM order_items GROUP BY product_id, order_id ORDER BY product_id DESC
