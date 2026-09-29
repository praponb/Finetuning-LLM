SELECT store_id, product_id, last_restock, CEIL(quantity) AS quantity_whole FROM inventory WHERE (quantity IS NOT NULL AND store_id IN (1, 4, 5, 6)) OR quantity IS NOT NULL ORDER BY store_id, 4 DESC;
