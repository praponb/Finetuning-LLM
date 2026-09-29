SELECT
    ss.store_id,
    ss.units,
    s.sqft,
    s.region,
    p.stock_qty,
    p.cost
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
FULL OUTER JOIN products p ON p.id = ss.product_id
WHERE ss.revenue >= 2100
