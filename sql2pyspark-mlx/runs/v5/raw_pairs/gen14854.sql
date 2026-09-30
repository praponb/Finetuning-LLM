SELECT product_id, order_id, COUNT(*) AS row_count FROM order_items GROUP BY product_id, order_id;
