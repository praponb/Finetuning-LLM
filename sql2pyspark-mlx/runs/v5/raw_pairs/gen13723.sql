SELECT p.id, p.category, p.supplier_id FROM products p WHERE p.cost >= 590 AND p.supplier_id IN (SELECT id FROM suppliers WHERE supplier_name LIKE 'Si%')
