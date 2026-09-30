SELECT p.id, p.price, p.product_name FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating >= 2.1) ORDER BY p.price DESC, id LIMIT 5;
