SELECT order_id, CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(quantity) AS quantity_count
FROM order_items
WHERE product_id IN (27, 33, 40) AND order_id IN (13, 19, 35, 39)
GROUP BY order_id, CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
