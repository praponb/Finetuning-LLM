SELECT id, product_id, ROUND(quantity * 0.85, 2) AS quantity_adjusted, ROUND(quantity * 0.85, 2) AS quantity_adjusted_2 FROM inventory ORDER BY quantity_adjusted, id LIMIT 10;
