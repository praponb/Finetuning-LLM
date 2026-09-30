SELECT CASE WHEN o.amount >= 1680 THEN 'high' WHEN o.amount >= 340 THEN 'mid' ELSE 'low' END AS amount_bucket, AVG(oi.quantity) AS avg_quantity
FROM order_items oi
FULL OUTER JOIN orders o ON oi.order_id = o.id
WHERE o.discount >= 0.13
GROUP BY CASE WHEN o.amount >= 1680 THEN 'high' WHEN o.amount >= 340 THEN 'mid' ELSE 'low' END
HAVING AVG(oi.quantity) > 3.3
ORDER BY amount_bucket;
