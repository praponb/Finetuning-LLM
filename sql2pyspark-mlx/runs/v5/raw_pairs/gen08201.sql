SELECT i.last_restock, s.store_name
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.id;
