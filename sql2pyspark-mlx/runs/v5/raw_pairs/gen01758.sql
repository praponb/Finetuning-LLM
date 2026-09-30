SELECT
    dim.brand,
    dim.category,
    COUNT(*) AS cnt,
    MIN(s.quantity) AS min_quantity,
    GROUPING_ID() AS level_id
FROM inventory s
LEFT JOIN products dim ON s.product_id = dim.id
GROUP BY GROUPING SETS ((dim.brand, dim.category), (dim.brand), ())
ORDER BY category;
