SELECT product_id, MAX(quantity) AS max_quantity FROM inventory GROUP BY product_id HAVING MAX(quantity) < 140;
