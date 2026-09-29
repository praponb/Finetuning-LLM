SELECT s.id, s.store_name
FROM stores s
WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id);
