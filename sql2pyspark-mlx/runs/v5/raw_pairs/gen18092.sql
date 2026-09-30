SELECT s.id, s.country, s.rating FROM suppliers s WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.brand = 'Initech');
