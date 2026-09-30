SELECT ss.id, ss.units, s.city, s.sqft FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id WHERE s.region = 'South';
