SELECT
    ss.revenue,
    p.supplier_id,
    p.category,
    s.region,
    s.sqft
FROM store_sales ss
RIGHT OUTER JOIN products p ON ss.product_id = p.id
LEFT OUTER JOIN stores s ON s.id = ss.store_id
WHERE p.stock_qty <= 303;
