SELECT i.id, i.store_id, s.region FROM inventory i INNER JOIN stores s ON i.store_id = s.id ORDER BY store_id
