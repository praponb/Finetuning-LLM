SELECT o.status, COUNT(s.id) AS shipment_count, ROUND((100 * SUM(CASE WHEN s.carrier = 'FedEx' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_fedex
FROM orders o
LEFT OUTER JOIN shipments s ON o.id = s.order_id
GROUP BY o.status
ORDER BY pct_fedex DESC
