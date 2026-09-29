SELECT products.id, products.cost FROM products WHERE products.supplier_id IN (SELECT id FROM suppliers WHERE rating < 4.2) ORDER BY products.id;
