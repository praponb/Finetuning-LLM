SELECT ss.id, ss.store_id FROM store_sales ss WHERE ss.store_id IN (1, 4) AND ss.store_id NOT IN (SELECT id FROM stores WHERE LOWER(store_name) LIKE '%irp%');
