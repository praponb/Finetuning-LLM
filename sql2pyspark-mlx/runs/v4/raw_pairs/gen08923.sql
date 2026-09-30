SELECT s.id, s.city, s.region
FROM stores s
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id);
