SELECT product_id, COUNT(*) row_count FROM order_items GROUP BY product_id ORDER BY product_id DESC
