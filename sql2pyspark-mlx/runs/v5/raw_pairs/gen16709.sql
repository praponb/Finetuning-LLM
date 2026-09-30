SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket, ROUND(AVG(quantity), 2) AS avg_quantity
FROM order_items
WHERE (unit_price BETWEEN 220 AND 300 AND unit_price BETWEEN 180 AND 250) OR quantity = 8
GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
ORDER BY quantity_bucket;
