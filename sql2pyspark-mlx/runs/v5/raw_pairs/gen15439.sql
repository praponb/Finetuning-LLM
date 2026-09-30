SELECT i.id, i.product_id, s.region, p.product_name FROM inventory i JOIN stores s ON i.store_id = s.id JOIN products p ON i.product_id = p.id;
