SELECT *, NTILE(4) OVER (PARTITION BY product_id ORDER BY quantity, id) AS quantity_bucket, AVG(quantity) OVER (PARTITION BY store_id) AS store_id_avg_quantity FROM inventory
