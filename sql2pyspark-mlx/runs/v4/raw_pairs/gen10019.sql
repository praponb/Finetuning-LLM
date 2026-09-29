SELECT ss.id, ss.store_id, s.opened_date, s.sqft, s.city, ss.units * ss.revenue AS revenue_per_unit FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id ORDER BY opened_date, id LIMIT 5;
