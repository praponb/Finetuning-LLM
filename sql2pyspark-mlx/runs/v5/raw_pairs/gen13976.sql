SELECT
    p.country,
    p.city,
    COUNT(*) AS cnt,
    AVG(a.stock_qty) AS avg_stock_qty
FROM products a
INNER JOIN suppliers p ON a.supplier_id = p.id
GROUP BY ROLLUP (p.country, p.city)
ORDER BY cnt NULLS FIRST
