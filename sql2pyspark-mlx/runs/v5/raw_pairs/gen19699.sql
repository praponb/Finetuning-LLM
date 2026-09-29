SELECT p.id, p.product_name
FROM products p
WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE LOWER(TRIM(supplier_name)) = 'lion city imports')
ORDER BY product_name DESC;
