SELECT o.id, o.channel, COUNT(s.id) shipment_count
FROM orders o
JOIN shipments s ON s.order_id = o.id
WHERE o.shipping_fee IS NOT NULL
GROUP BY o.id, o.channel
ORDER BY o.id DESC, channel;
