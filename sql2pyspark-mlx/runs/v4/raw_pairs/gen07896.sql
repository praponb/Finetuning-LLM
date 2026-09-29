SELECT s.id, s.store_name, s.city FROM stores s WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id);
