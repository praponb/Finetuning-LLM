SELECT p.id, p.product_name
FROM products p
WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating BETWEEN 3.3 AND 3.5)
