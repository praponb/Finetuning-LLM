SELECT i.quantity, i.last_restock, p.brand, s.city, s.sqft FROM inventory i INNER JOIN products p ON p.id = i.product_id INNER JOIN stores s ON i.store_id = s.id ORDER BY p.brand ASC;
