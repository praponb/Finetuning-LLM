SELECT p.id, p.cost, p.price, p.supplier_id FROM products p WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE city IS NULL);
