SELECT s.id, s.city, s.store_name, s.region FROM stores s WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id AND i.quantity >= 151)
