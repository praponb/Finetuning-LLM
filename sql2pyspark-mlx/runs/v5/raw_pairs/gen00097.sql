SELECT ss.id, ss.revenue, s.store_name, s.region FROM store_sales ss JOIN stores s ON ss.store_id = s.id ORDER BY ss.revenue DESC, id ASC LIMIT 1
