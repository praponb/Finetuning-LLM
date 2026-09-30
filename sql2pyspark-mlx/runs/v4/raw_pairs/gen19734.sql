SELECT
    s.id,
    s.store_name,
    s.city,
    s.region
FROM stores s
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id)
ORDER BY city DESC, region
