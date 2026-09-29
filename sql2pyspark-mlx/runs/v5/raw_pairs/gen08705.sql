SELECT
    ss.id,
    ss.units,
    s.opened_date,
    s.store_name,
    p.product_name,
    p.brand
FROM store_sales ss
LEFT OUTER JOIN stores s ON s.id = ss.store_id
LEFT OUTER JOIN products p ON ss.product_id = p.id
WHERE p.supplier_id = 17
ORDER BY s.opened_date;
