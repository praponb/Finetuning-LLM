SELECT product_id, SUM(unit_price) AS total_unit_price FROM order_items GROUP BY product_id ORDER BY product_id DESC
