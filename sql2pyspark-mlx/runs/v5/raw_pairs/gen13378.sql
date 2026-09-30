SELECT s.id, s.city, s.store_name
FROM stores s
WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.product_id = 7)
ORDER BY s.store_name DESC, id
LIMIT 20;
