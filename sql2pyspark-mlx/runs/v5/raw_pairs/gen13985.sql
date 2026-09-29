SELECT s.id, s.store_name
FROM stores s
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id)
ORDER BY s.id
