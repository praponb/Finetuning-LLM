SELECT p.id, p.cost, s.city FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id WHERE ARRAY_CONTAINS(p.tags, 'sale') ORDER BY cost ASC;
