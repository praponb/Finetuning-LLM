SELECT p.id, p.price
FROM products p
WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE city IN ('New York', 'Singapore'))
