SELECT s.id, s.region FROM stores s WHERE s.region != 'North' AND EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id) ORDER BY id;
