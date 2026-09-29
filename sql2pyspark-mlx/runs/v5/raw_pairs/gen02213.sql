SELECT
    s.country,
    s.city,
    COUNT(*) AS cnt,
    SUM(a.price) AS total_price,
    MIN(a.stock_qty) AS min_stock_qty,
    GROUPING_ID() AS gid
FROM products a
JOIN suppliers s ON a.supplier_id = s.id
GROUP BY GROUPING SETS ((s.country, s.city), (s.country), ())
