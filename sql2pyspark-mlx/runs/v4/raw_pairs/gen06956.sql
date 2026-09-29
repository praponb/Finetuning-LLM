SELECT s.id, s.sqft, s.region FROM stores s WHERE s.city NOT IN ('Singapore', 'Tokyo', 'New York') AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id);
