SELECT o.status, CASE WHEN o.shipping_fee >= 43 THEN 'high' WHEN o.shipping_fee >= 13 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket, MAX(s.cost) AS max_cost
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id
WHERE o.shipping_fee BETWEEN 26 AND 33
GROUP BY o.status, CASE WHEN o.shipping_fee >= 43 THEN 'high' WHEN o.shipping_fee >= 13 THEN 'mid' ELSE 'low' END;
