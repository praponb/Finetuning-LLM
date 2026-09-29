SELECT
    store_id,
    CASE WHEN quantity >= 260 THEN 'high' WHEN quantity >= 110 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    product_id,
    AVG(quantity) AS avg_quantity,
    SUM(quantity) AS total_quantity,
    COUNT(*) AS num_inventory
FROM inventory
WHERE quantity BETWEEN 73 AND 188 AND product_id = 1 AND quantity != 238
GROUP BY 1, 2, 3
ORDER BY total_quantity DESC
