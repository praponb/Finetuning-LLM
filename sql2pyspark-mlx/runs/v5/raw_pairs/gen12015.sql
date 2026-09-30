SELECT
    ss.product_id,
    ss.sale_date,
    s.city,
    s.sqft,
    p.supplier_id
FROM store_sales ss
RIGHT OUTER JOIN stores s ON s.id = ss.store_id
JOIN products p ON p.id = ss.product_id
WHERE ss.store_id IN (2, 3, 4)
