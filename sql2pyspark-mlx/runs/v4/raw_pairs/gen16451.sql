SELECT
    store_id,
    product_id,
    CASE WHEN quantity >= 182 THEN 'high' WHEN quantity >= 116 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    SUM(quantity) AS total_quantity,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM inventory
WHERE store_id IN (3, 5) AND (store_id IN (1, 2, 3, 4) OR quantity > 215)
GROUP BY store_id, product_id, CASE WHEN quantity >= 182 THEN 'high' WHEN quantity >= 116 THEN 'mid' ELSE 'low' END
ORDER BY 2 DESC;
