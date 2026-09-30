SELECT s.id, s.city
FROM stores s
WHERE s.city IN ('Bangkok', 'Singapore') AND EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id)
ORDER BY s.id DESC;
