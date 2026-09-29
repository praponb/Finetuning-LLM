SELECT s.id, s.city FROM suppliers s WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND ARRAY_CONTAINS(p.tags, 'eco'))
