SELECT i.id, i.product_id, i.store_id, s.city FROM inventory i JOIN stores s ON i.store_id = s.id ORDER BY s.city, id LIMIT 5;
