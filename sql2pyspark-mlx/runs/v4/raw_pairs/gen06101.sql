SELECT s.id, s.region
FROM stores s
WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.sale_date < '2023-11-01')
ORDER BY region
