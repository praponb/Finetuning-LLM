SELECT id, store_id, last_restock, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock, id) AS last_restock_row FROM inventory WHERE quantity BETWEEN 95 AND 224;
