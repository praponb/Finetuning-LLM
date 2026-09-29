SELECT i.id, i.quantity, i.store_id, p.launch_date, s.country FROM inventory i INNER JOIN products p ON i.product_id = p.id INNER JOIN suppliers s ON p.supplier_id = s.id
