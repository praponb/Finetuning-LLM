SELECT CASE WHEN o.shipping_fee >= 39 THEN 'high' WHEN o.shipping_fee >= 8 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket, AVG(s.cost) AS avg_cost
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
LEFT JOIN stores st ON o.store_id = st.id
WHERE o.shipping_fee <= 30
GROUP BY CASE WHEN o.shipping_fee >= 39 THEN 'high' WHEN o.shipping_fee >= 8 THEN 'mid' ELSE 'low' END
ORDER BY avg_cost ASC, shipping_fee_bucket ASC
LIMIT 5;
