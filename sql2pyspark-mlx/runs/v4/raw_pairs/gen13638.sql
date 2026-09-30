SELECT product_id, store_id, MAX(last_restock) AS last_last_restock
FROM inventory
GROUP BY 1, 2
