SELECT
    CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END quantity_bucket,
    COUNT(quantity) quantity_count,
    AVG(quantity) avg_quantity,
    SUM(unit_price) total_unit_price
FROM order_items
GROUP BY CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
ORDER BY quantity_count;
