SELECT
    p.id,
    p.category,
    p.launch_date,
    p.stock_qty
FROM products p
WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE city NOT IN ('New York', 'London'))
