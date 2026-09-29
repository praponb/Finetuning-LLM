SELECT p.id, p.supplier_id, p.brand, p.product_name FROM products p WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE city <> 'Singapore');
