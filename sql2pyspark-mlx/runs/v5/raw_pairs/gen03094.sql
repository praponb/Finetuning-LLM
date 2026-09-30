SELECT i.id, i.last_restock, i.store_id, s.city, s.opened_date, p.cost FROM inventory i LEFT JOIN stores s ON s.id = i.store_id LEFT JOIN products p ON i.product_id = p.id;
