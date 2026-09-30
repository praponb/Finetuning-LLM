SELECT * FROM inventory QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last_restock, id) = 1
