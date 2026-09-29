SELECT s.id, s.store_name, s.city
FROM stores s
WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id)
