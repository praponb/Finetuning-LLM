SELECT CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket, SUM(quantity) AS total_quantity, ROUND(SUM(quantity) * 100.0 / SUM(SUM(quantity)) OVER (), 2) AS pct_of_total
FROM order_items
WHERE order_id IN (14, 23)
GROUP BY CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END
