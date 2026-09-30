SELECT product_id, SUM(quantity) AS total_quantity FROM inventory GROUP BY product_id ORDER BY total_quantity DESC, product_id LIMIT 20;
