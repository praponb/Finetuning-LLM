SELECT product_id, order_id, SUM(unit_price) AS total_unit_price, COUNT(unit_price) AS unit_price_count FROM order_items GROUP BY product_id, order_id ORDER BY order_id DESC
