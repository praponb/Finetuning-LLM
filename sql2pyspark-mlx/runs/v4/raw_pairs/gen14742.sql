SELECT id, store_id, last_restock, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY last_restock) AS last_restock_dense_rank FROM inventory ORDER BY last_restock_dense_rank DESC;
