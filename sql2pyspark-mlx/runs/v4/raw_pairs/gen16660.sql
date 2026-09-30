SELECT id, store_id, quantity, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC) AS dense_rnk FROM inventory ORDER BY 4 DESC
