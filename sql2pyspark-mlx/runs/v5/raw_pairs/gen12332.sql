SELECT p.id, p.cost, s.supplier_name
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
