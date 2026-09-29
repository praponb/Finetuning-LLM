SELECT i.store_id, i.product_id, p.cost, s.supplier_name FROM inventory i LEFT OUTER JOIN products p ON i.product_id = p.id LEFT OUTER JOIN suppliers s ON s.id = p.supplier_id WHERE p.cost < 250;
