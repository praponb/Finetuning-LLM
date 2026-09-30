SELECT s.id, s.sqft, s.store_name
FROM stores s
WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id AND i.last_restock >= '2025-09-01');
