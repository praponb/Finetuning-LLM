SELECT last_restock, store_id, COALESCE(quantity, 0) AS quantity_filled
FROM inventory
WHERE store_id IN (2, 3, 5);
