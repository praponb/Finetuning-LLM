SELECT id, store_id, units, MIN(units) OVER (PARTITION BY store_id) AS store_id_min_units, COUNT(*) OVER (PARTITION BY store_id) AS store_id_count FROM store_sales;
