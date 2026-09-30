SELECT product_id, quantity_bucket, stddev_quantity
FROM (
    SELECT product_id, CASE WHEN quantity >= 6 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM order_items
    GROUP BY product_id, CASE WHEN quantity >= 6 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
) AS x
WHERE stddev_quantity < 33
ORDER BY 1 DESC, quantity_bucket ASC
LIMIT 10
