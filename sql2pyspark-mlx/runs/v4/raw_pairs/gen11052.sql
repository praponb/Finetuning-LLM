SELECT s.id, s.sqft, s.region, s.store_name FROM stores s WHERE s.region = 'East' AND EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id AND i.product_id IN (3, 4, 7))
