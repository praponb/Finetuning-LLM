SELECT s.id, s.city FROM stores s WHERE s.city = 'New York' AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.product_id IN (5, 9)) ORDER BY city
