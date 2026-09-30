SELECT i.id, i.last_restock, s.opened_date
FROM inventory i
JOIN stores s ON i.store_id = s.id;
