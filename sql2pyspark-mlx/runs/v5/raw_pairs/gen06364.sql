SELECT
    s.city,
    s.country,
    src.brand,
    COUNT(*) AS num_rows,
    MAX(src.stock_qty) AS max_stock_qty
FROM products src
JOIN suppliers s ON src.supplier_id = s.id
GROUP BY s.city, s.country, src.brand
