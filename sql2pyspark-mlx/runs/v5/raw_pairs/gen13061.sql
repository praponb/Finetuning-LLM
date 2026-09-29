SELECT s.id, s.opened_date
FROM stores s
WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id)
