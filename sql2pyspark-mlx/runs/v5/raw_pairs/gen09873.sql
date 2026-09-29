SELECT i.id, i.product_id, s.region, s.sqft, p.product_name FROM inventory i INNER JOIN stores s ON i.store_id = s.id FULL OUTER JOIN products p ON i.product_id = p.id;
