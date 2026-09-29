SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket, order_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM order_items
GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END, order_id
HAVING stddev_quantity >= 72
ORDER BY stddev_quantity
