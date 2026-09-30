SELECT MAX(quantity) - MIN(quantity) AS quantity_range, SUM(quantity) AS total_quantity, COUNT(quantity) AS quantity_count FROM inventory ORDER BY total_quantity;
