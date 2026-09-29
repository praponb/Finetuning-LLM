SELECT i.quantity, i.store_id, p.stock_qty, s.sqft FROM inventory i INNER JOIN products p ON i.product_id = p.id INNER JOIN stores s ON i.store_id = s.id WHERE LENGTH(s.store_name) >= 13
