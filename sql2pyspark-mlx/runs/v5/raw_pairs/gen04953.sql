SELECT
    d.category,
    d.brand,
    COUNT(*) AS cnt,
    ROUND(SUM(a.quantity), 2) AS total_quantity
FROM order_items a
INNER JOIN products d ON a.product_id = d.id
GROUP BY GROUPING SETS ((d.category, d.brand), (d.category), ());
