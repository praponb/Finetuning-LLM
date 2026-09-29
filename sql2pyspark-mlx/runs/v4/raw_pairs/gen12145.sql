SELECT s.id, s.opened_date, s.store_name FROM stores s WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.product_id IN (1, 3, 4, 8)) ORDER BY s.store_name DESC
