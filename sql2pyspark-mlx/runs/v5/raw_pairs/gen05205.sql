SELECT
    s.id,
    s.city,
    COUNT(p.id) AS product_count,
    COUNT(DISTINCT p.stock_qty) AS unique_suppliers,
    MAX(p.cost) - MIN(p.cost) AS cost_range
FROM suppliers s
LEFT JOIN products p ON p.supplier_id = s.id
WHERE p.category IN ('Sports', 'Home')
GROUP BY s.id, s.city
ORDER BY unique_suppliers DESC
