SELECT p.id, p.brand, p.product_name
FROM products p
WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating BETWEEN 2.1 AND 3.3);
