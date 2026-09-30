SELECT id, last_restock, product_id, CASE WHEN quantity >= 216 THEN 'High' WHEN quantity >= 37 THEN 'Medium' ELSE 'Low' END quantity_level FROM inventory WHERE store_id IN (1, 5);
