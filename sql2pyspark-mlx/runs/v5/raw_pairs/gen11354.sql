SELECT CASE WHEN o.amount >= 1600 THEN 'high' WHEN o.amount >= 680 THEN 'mid' ELSE 'low' END AS amount_bucket, COUNT(oi.quantity) AS quantity_count
FROM order_items oi
LEFT JOIN products p ON p.id = oi.product_id
RIGHT JOIN orders o ON oi.order_id = o.id
GROUP BY CASE WHEN o.amount >= 1600 THEN 'high' WHEN o.amount >= 680 THEN 'mid' ELSE 'low' END
HAVING COUNT(oi.quantity) >= 5;
