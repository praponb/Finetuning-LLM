SELECT s.id, s.store_name, s.opened_date FROM stores s WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id) ORDER BY store_name ASC
