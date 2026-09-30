SELECT CASE WHEN ss.units >= 33 THEN 'high' WHEN ss.units >= 15 THEN 'mid' ELSE 'low' END AS units_bucket, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity, MIN(i.quantity) AS min_quantity
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id IN (3, 9)
GROUP BY CASE WHEN ss.units >= 33 THEN 'high' WHEN ss.units >= 15 THEN 'mid' ELSE 'low' END
ORDER BY stddev_quantity DESC
