SELECT product_id, store_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, MAX(last_restock) AS last_last_restock FROM inventory GROUP BY product_id, store_id ORDER BY product_id
