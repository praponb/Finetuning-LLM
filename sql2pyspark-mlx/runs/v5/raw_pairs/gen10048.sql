SELECT CASE WHEN ss.units >= 43 THEN 'high' WHEN ss.units >= 10 THEN 'mid' ELSE 'low' END AS units_bucket, MAX(i.last_restock) AS last_last_restock
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY CASE WHEN ss.units >= 43 THEN 'high' WHEN ss.units >= 10 THEN 'mid' ELSE 'low' END
ORDER BY units_bucket DESC
LIMIT 5;
