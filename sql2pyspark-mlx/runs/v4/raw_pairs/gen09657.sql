SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(*) AS cnt, SUM(unit_price) AS total_unit_price
FROM order_items
WHERE product_id IN (4, 30) AND (quantity = 4 OR order_id IN (16, 30, 36))
GROUP BY quantity_bucket
ORDER BY 3 DESC;
