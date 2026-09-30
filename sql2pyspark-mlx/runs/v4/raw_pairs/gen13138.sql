SELECT CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket, SUM(quantity) AS total_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
FROM order_items
WHERE unit_price BETWEEN 270 AND 380 OR product_id IS NULL
GROUP BY CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
HAVING total_quantity > 41
