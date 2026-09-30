SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(unit_price) AS unit_price_count, MAX(quantity) - MIN(quantity) AS quantity_range
FROM order_items
GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
HAVING quantity_range > 58
ORDER BY quantity_range
