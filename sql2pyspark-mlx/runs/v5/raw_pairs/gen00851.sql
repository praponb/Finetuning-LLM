SELECT i.quantity, i.product_id, s.city, s.region FROM inventory i JOIN stores s ON i.store_id = s.id WHERE s.region = 'West' AND s.store_name LIKE '%Sia%';
