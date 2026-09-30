SELECT i.id, i.last_restock, s.opened_date, s.store_name, s.region FROM inventory i JOIN stores s ON i.store_id = s.id WHERE s.store_name IS NULL
