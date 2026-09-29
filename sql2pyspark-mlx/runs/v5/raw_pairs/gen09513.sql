SELECT
    s.country,
    COUNT(p.id) AS product_count,
    SUM(p.cost) AS total_cost,
    SUM(p.stock_qty) AS total_stock_qty
FROM suppliers s
JOIN products p ON p.supplier_id = s.id
WHERE ARRAY_CONTAINS(p.tags, 'clearance')
GROUP BY s.country
HAVING total_stock_qty > 800
ORDER BY product_count DESC, country DESC;
