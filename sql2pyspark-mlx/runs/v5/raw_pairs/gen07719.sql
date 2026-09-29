SELECT s.id, s.city
FROM stores s
WHERE s.opened_date >= '2025-05-01' AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id)
