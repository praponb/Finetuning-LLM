SELECT
    p.category,
    COUNT(*) AS num_rows,
    ROUND(SUM(e.quantity), 2) AS total_quantity
FROM inventory e
LEFT JOIN products p ON e.product_id = p.id
GROUP BY p.category
