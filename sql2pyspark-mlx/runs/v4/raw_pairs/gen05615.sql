SELECT CASE WHEN unit_price >= 420 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END AS unit_price_bucket, CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket, MIN(unit_price) AS min_unit_price
FROM order_items
WHERE product_id IS NULL
GROUP BY CASE WHEN unit_price >= 420 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END, CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END;
