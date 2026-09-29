SELECT s.id, s.opened_date
FROM stores s
WHERE s.region = 'West' AND EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id);
