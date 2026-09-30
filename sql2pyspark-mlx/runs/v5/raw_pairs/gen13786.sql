SELECT ss.store_id, s.sqft, s.region, s.store_name FROM store_sales ss JOIN stores s ON ss.store_id = s.id
