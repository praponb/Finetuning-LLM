SELECT s.city, MIN(ss.units) AS min_units FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id GROUP BY s.city
