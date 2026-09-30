SELECT i.id, i.last_restock, s.opened_date, s.sqft FROM inventory i JOIN stores s ON i.store_id = s.id
