SELECT s.region, MAX(i.quantity) max_quantity FROM inventory i JOIN stores s ON i.store_id = s.id GROUP BY s.region ORDER BY max_quantity DESC;
