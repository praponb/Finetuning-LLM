SELECT id, product_id, last_restock, store_id, CAST(quantity AS DOUBLE) AS quantity_num FROM inventory WHERE store_id IN (1, 4, 5, 6) ORDER BY product_id DESC;
