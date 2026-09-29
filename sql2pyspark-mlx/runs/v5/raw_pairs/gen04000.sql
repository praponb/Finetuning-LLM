SELECT
    ss.id,
    ss.product_id,
    ss.sale_date,
    s.opened_date,
    s.store_name,
    p.supplier_id,
    ROUND(ss.units, 1) AS units_rounded
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON p.id = ss.product_id
WHERE ss.revenue > 3700
ORDER BY opened_date, 3;
