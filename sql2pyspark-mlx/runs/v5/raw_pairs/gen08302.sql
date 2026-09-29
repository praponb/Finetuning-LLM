SELECT s.id, s.region FROM stores s WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.channel = 'partner') ORDER BY s.region;
