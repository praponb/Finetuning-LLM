SELECT
    s.id,
    s.country,
    COUNT(p.id) AS product_count,
    AVG(p.stock_qty) AS avg_stock_qty,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.brand))) AS brand_values
FROM suppliers s
LEFT JOIN products p ON s.id = p.supplier_id
WHERE p.price >= 830
GROUP BY s.id, s.country
ORDER BY avg_stock_qty;
