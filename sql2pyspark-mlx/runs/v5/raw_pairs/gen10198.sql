SELECT p.id, p.stock_qty, p.supplier_id
FROM products p
WHERE p.supplier_id IN (1, 8, 17) AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE supplier_name IS NOT NULL);
