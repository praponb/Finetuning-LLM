SELECT YEAR(p.launch_date) AS launch_year, CASE WHEN p.price >= 1050 THEN 'high' WHEN p.price >= 480 THEN 'mid' ELSE 'low' END AS price_bucket, MIN(oi.quantity) AS min_quantity
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON p.id = oi.product_id
GROUP BY YEAR(p.launch_date), CASE WHEN p.price >= 1050 THEN 'high' WHEN p.price >= 480 THEN 'mid' ELSE 'low' END
HAVING MIN(oi.quantity) >= 6.8
ORDER BY launch_year DESC;
