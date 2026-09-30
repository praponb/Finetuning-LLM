SELECT p.id, p.cost, p.category
FROM products p
WHERE p.supplier_id IS NULL AND p.supplier_id IN (SELECT id FROM suppliers WHERE country IN ('DE', 'JP'));
