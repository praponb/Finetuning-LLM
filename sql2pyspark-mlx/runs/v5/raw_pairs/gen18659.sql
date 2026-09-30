SELECT
    dim.category,
    dim.brand,
    COUNT(*) AS num_rows,
    MAX(s.quantity) AS max_quantity,
    GROUPING_ID() AS level_id
FROM inventory s
LEFT JOIN products dim ON s.product_id = dim.id
GROUP BY ROLLUP (dim.category, dim.brand)
ORDER BY max_quantity;
