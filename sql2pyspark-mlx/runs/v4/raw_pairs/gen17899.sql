SELECT p.id, p.supplier_id, p.category FROM products p WHERE p.product_name LIKE 'Lap%' AND p.supplier_id IN (SELECT id FROM suppliers WHERE city <> 'Singapore') ORDER BY category DESC
