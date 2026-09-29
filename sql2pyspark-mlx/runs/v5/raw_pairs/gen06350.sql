SELECT
    d.category,
    COUNT(*) AS cnt,
    ROUND(SUM(s.quantity), 2) AS total_quantity
FROM order_items s
LEFT JOIN products d ON s.product_id = d.id
GROUP BY d.category;
