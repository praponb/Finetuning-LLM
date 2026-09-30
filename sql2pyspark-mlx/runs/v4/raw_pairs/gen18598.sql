SELECT s.id, s.region, s.sqft FROM stores s WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id)
