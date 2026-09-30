SELECT p.id, p.brand, p.product_name FROM products p WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE supplier_name LIKE '%City%')
