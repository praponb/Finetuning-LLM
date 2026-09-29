SELECT
    ss.id,
    ss.revenue,
    s.store_name,
    s.region,
    p.stock_qty,
    p.cost
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
JOIN products p ON ss.product_id = p.id
WHERE ARRAY_CONTAINS(p.tags, 'eco') OR p.stock_qty < 339
ORDER BY p.stock_qty DESC;
