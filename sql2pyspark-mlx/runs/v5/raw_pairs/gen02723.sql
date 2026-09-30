SELECT ss.id, ss.product_id, ss.revenue, s.store_name, s.sqft FROM store_sales ss RIGHT JOIN stores s ON ss.store_id = s.id WHERE ss.units < 25 ORDER BY s.store_name DESC, ss.revenue ASC
