SELECT o.status, o.channel, o.store_id, s.order_id FROM orders o LEFT OUTER JOIN shipments s ON s.order_id = o.id ORDER BY channel
