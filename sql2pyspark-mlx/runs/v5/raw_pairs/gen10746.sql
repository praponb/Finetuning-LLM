SELECT ss.id, ss.units, s.store_name, s.opened_date FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id WHERE ss.store_id IN (1, 4) ORDER BY s.opened_date DESC;
