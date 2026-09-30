SELECT last_restock, product_id FROM inventory WHERE last_restock <= '2025-04-01' INTERSECT SELECT last_restock, product_id FROM inventory WHERE quantity >= 194 ORDER BY product_id;
