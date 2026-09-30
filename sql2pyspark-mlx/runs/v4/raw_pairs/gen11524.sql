SELECT p.id, p.supplier_id
FROM products p
WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE city IN ('Berlin', 'London', 'New York'))
