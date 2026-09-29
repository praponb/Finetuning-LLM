SELECT id, store_id, COUNT(*) OVER (PARTITION BY store_id) AS store_id_count FROM store_sales WHERE store_id IN (1, 2, 3, 5)
