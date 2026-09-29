SELECT CASE WHEN o.shipping_fee >= 31 THEN 'high' WHEN o.shipping_fee >= 14 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket, COUNT(*) AS total_count
FROM shipments s
LEFT JOIN orders o ON o.id = s.order_id
WHERE s.carrier NOT IN ('DHL', 'Flash', 'FedEx')
GROUP BY CASE WHEN o.shipping_fee >= 31 THEN 'high' WHEN o.shipping_fee >= 14 THEN 'mid' ELSE 'low' END
HAVING total_count >= 4
ORDER BY shipping_fee_bucket DESC
LIMIT 1;
