SELECT i.id, i.product_id, i.quantity, i.store_id, s.opened_date, s.store_name FROM inventory i LEFT OUTER JOIN stores s ON i.store_id = s.id;
