SELECT * FROM (SELECT product_id, store_id, MAX(quantity) max_quantity FROM inventory GROUP BY product_id, store_id) agg WHERE max_quantity < 120 ORDER BY product_id DESC, store_id;
