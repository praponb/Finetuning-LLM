SELECT p.id, p.brand FROM products p WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE rating >= 3.2) ORDER BY 1
