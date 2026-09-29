SELECT id, product_id, store_id, units, MAX(units) OVER (PARTITION BY product_id, store_id) AS product_id_max_units FROM store_sales
