SELECT CASE WHEN ss.units >= 43 THEN 'high' WHEN ss.units >= 21 THEN 'mid' ELSE 'low' END AS units_bucket, SUM(i.quantity) AS total_quantity
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.units < 16
GROUP BY CASE WHEN ss.units >= 43 THEN 'high' WHEN ss.units >= 21 THEN 'mid' ELSE 'low' END;
