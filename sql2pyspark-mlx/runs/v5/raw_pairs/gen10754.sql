SELECT id, product_id, store_id, units, RANK() OVER (PARTITION BY product_id, store_id ORDER BY units DESC) AS units_rank FROM store_sales WHERE units = 21;
