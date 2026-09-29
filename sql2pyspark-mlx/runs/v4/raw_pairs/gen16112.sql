SELECT p.id, p.product_name, p.launch_date, p.category FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE LOWER(supplier_name) LIKE '%aci%')
