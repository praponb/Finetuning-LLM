SELECT s.* FROM stores s WHERE NOT EXISTS (SELECT 1 FROM orders c WHERE c.store_id = s.id AND c.store_id IS NULL)
