SELECT o.shipping_fee, o.channel, s.cost FROM orders o LEFT OUTER JOIN shipments s ON s.order_id = o.id WHERE o.customer_id IN (10, 23, 24) ORDER BY channel
