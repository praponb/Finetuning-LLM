SELECT s.id, s.region, s.sqft, s.store_name FROM stores s WHERE s.city IN ('Berlin', 'Tokyo', 'New York') AND EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id)
