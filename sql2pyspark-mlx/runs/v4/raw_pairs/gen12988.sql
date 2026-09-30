SELECT
    p.id,
    p.category,
    p.stock_qty,
    p.supplier_id
FROM products p
WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating > 3.8)
