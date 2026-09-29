SELECT p.id, p.price FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating <= 2.8) ORDER BY p.price, id LIMIT 10;
