SELECT id, store_id FROM store_sales WHERE store_id IN (1, 4, 5) INTERSECT SELECT id, store_id FROM store_sales WHERE revenue IS NULL ORDER BY id;
