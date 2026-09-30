SELECT unit_price_bucket, revenue
FROM (
    SELECT CASE WHEN unit_price >= 380 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END unit_price_bucket, SUM(quantity * unit_price) revenue
    FROM order_items
    GROUP BY CASE WHEN unit_price >= 380 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END
) s
WHERE revenue < 28
ORDER BY unit_price_bucket
LIMIT 50
