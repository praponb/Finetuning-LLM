SELECT
    CASE WHEN quantity >= 240 THEN 'high' WHEN quantity >= 65 THEN 'mid' ELSE 'low' END quantity_bucket,
    DATE_TRUNC('MONTH', last_restock) last_restock_month,
    AVG(quantity) avg_quantity,
    COUNT(DISTINCT quantity) unique_stores
FROM inventory
WHERE quantity IS NOT NULL OR quantity > 219
GROUP BY CASE WHEN quantity >= 240 THEN 'high' WHEN quantity >= 65 THEN 'mid' ELSE 'low' END, DATE_TRUNC('MONTH', last_restock)
ORDER BY last_restock_month;
