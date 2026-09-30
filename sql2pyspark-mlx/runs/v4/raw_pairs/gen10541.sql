SELECT i.id, i.last_restock, i.store_id, i.quantity, s.region FROM inventory i JOIN stores s ON s.id = i.store_id ORDER BY s.region, i.last_restock DESC;
