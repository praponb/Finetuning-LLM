SELECT s.id, s.region, s.store_name, s.sqft FROM stores s WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.channel IN ('partner', 'store'))
