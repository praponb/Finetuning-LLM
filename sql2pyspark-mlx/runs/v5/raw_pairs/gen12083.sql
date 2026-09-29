SELECT last_restock, store_id, FLOOR(quantity) AS quantity_whole
FROM inventory
WHERE YEAR(last_restock) = 2024;
