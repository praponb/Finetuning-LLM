SELECT ss.id, ss.units, s.opened_date, s.sqft FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id ORDER BY sqft
