SELECT orders.channel, SUM(CASE WHEN shipments.carrier = 'FedEx' THEN 1 ELSE 0 END) AS fedex_count
FROM shipments
INNER JOIN orders ON shipments.order_id = orders.id
GROUP BY orders.channel
