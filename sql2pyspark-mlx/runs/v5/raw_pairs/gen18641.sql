SELECT CASE WHEN o.shipping_fee >= 30 THEN 'high' WHEN o.shipping_fee >= 12 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket, COUNT(CASE WHEN s.carrier = 'Flash' THEN 1 END) AS flash_count
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
JOIN stores st ON o.store_id = st.id
GROUP BY CASE WHEN o.shipping_fee >= 30 THEN 'high' WHEN o.shipping_fee >= 12 THEN 'mid' ELSE 'low' END
ORDER BY shipping_fee_bucket
