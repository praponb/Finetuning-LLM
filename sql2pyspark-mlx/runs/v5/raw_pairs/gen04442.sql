SELECT
    p.supplier_id,
    p.category,
    ROUND(AVG(ss.revenue), 2) AS avg_revenue,
    COUNT(*) AS store_sale_count
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
RIGHT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY p.supplier_id, p.category
HAVING COUNT(*) > 1
ORDER BY p.category;
