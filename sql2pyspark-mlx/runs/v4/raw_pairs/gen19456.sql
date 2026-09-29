SELECT
    p.category,
    p.stock_qty,
    p.supplier_id,
    s.supplier_name,
    s.rating,
    s.country
FROM products p
RIGHT JOIN suppliers s ON p.supplier_id = s.id
WHERE UPPER(s.supplier_name) LIKE '%ION%'
