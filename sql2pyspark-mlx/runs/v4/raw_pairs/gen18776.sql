SELECT
    product_id,
    CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    COUNT(*) AS total_count,
    COUNT(quantity) AS quantity_count
FROM order_items
GROUP BY product_id, CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END
HAVING MAX(quantity) >= 6.5
ORDER BY total_count DESC
