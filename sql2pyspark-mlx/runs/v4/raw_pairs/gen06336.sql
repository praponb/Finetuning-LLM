SELECT id, quantity, ROUND(quantity * 0.9, 2) AS quantity_scaled FROM inventory ORDER BY quantity_scaled, id LIMIT 10;
