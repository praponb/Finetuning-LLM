SELECT i.id, i.quantity, s.region FROM inventory i LEFT JOIN stores s ON i.store_id = s.id ORDER BY quantity ASC, id DESC LIMIT 20;
