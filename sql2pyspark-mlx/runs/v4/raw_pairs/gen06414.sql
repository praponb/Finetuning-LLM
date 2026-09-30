SELECT i.id, i.store_id, i.product_id, p.product_name, p.brand, s.supplier_name FROM inventory i INNER JOIN products p ON i.product_id = p.id LEFT JOIN suppliers s ON p.supplier_id = s.id
