SELECT
    p.id,
    p.stock_qty,
    s.supplier_name,
    s.country
FROM products p
FULL OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.supplier_id = 30 AND s.country = 'UK'
