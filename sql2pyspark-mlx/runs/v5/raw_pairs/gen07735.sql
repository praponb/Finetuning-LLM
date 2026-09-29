SELECT p.id, p.cost, p.product_name FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE city IS NOT NULL);
