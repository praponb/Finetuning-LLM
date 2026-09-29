SELECT s.id, s.city, s.region, s.sqft FROM stores s WHERE s.id IN (SELECT store_id FROM inventory) ORDER BY id;
