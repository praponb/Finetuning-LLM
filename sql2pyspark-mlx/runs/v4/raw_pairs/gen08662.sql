SELECT i.id, i.quantity, s.sqft, p.category, p.launch_date FROM inventory i LEFT JOIN stores s ON i.store_id = s.id JOIN products p ON i.product_id = p.id WHERE i.quantity = 156;
