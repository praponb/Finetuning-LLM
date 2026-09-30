SELECT ss.product_id, s.sqft, s.store_name, p.cost FROM store_sales ss LEFT OUTER JOIN stores s ON ss.store_id = s.id INNER JOIN products p ON ss.product_id = p.id ORDER BY store_name, s.sqft;
