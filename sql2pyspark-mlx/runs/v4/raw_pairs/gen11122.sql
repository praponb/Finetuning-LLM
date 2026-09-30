SELECT
    ss.store_id,
    p.supplier_id,
    s.rating,
    s.supplier_name,
    ROUND(ss.revenue, 2) revenue_rounded
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
LEFT JOIN suppliers s ON s.id = p.supplier_id
ORDER BY revenue_rounded DESC;
