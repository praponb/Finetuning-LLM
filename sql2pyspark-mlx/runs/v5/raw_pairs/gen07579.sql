SELECT CASE WHEN orders.amount >= 1450 THEN 'high' WHEN orders.amount >= 380 THEN 'mid' ELSE 'low' END AS amount_bucket, COUNT(shipments.cost) AS cost_count, ROUND(STDDEV(shipments.cost), 2) AS stddev_cost
FROM shipments
LEFT JOIN orders ON shipments.order_id = orders.id
WHERE orders.customer_id IN (3, 15, 16, 39)
GROUP BY CASE WHEN orders.amount >= 1450 THEN 'high' WHEN orders.amount >= 380 THEN 'mid' ELSE 'low' END
ORDER BY amount_bucket
