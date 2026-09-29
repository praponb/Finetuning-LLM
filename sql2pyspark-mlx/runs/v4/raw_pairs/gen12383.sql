SELECT ss.id, ss.product_id, s.store_name, s.sqft FROM store_sales ss JOIN stores s ON ss.store_id = s.id WHERE ss.product_id IN (1, 3, 5, 10) ORDER BY product_id DESC, id DESC;
