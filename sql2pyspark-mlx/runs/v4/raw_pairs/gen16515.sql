SELECT
    ss.sale_date,
    ss.store_id,
    s.sqft,
    p.product_name,
    p.cost,
    p.stock_qty - p.price AS stock_qty_minus_price
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
LEFT JOIN products p ON p.id = ss.product_id
WHERE s.city IN ('Tokyo', 'Singapore', 'Bangkok')
ORDER BY s.sqft DESC;
