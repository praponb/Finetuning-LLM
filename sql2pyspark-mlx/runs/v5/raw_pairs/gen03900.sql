SELECT i.id, i.quantity, i.store_id, s.sqft, s.opened_date, p.brand FROM inventory i INNER JOIN stores s ON i.store_id = s.id LEFT OUTER JOIN products p ON p.id = i.product_id
