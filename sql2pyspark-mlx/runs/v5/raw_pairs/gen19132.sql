SELECT
    s.id,
    s.city,
    s.region,
    s.sqft
FROM stores s
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.store_id = 16)
ORDER BY sqft
