SELECT CASE WHEN unit_price >= 420 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END unit_price_bucket, ROUND(AVG(quantity), 2) avg_quantity
FROM order_items
WHERE (order_id IN (27, 29) AND unit_price >= 210) OR product_id = 4
GROUP BY unit_price_bucket
