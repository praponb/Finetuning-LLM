SELECT CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket, MIN(quantity) AS min_quantity
FROM order_items
GROUP BY quantity_bucket
