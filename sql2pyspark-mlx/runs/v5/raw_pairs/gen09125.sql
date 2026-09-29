SELECT
    p.id,
    p.cost,
    s.city,
    CASE WHEN p.stock_qty >= 309 THEN 'High' WHEN p.stock_qty >= 112 THEN 'Medium' ELSE 'Low' END AS stock_qty_band
FROM products p
JOIN suppliers s ON p.supplier_id = s.id
WHERE s.country = 'US' AND p.category = 'Books';
