SELECT
    s.country,
    COUNT(p.id) AS product_count,
    SUM(CASE WHEN p.brand = 'Globex' THEN p.stock_qty ELSE 0 END) AS globex_stock_qty,
    SUM(p.stock_qty) AS total_stock_qty
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id
GROUP BY s.country
