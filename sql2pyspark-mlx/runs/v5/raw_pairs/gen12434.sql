SELECT o.shipping_fee, o.channel, o.order_date, s.carrier, s.delivered_date, s.cost FROM orders o LEFT JOIN shipments s ON s.order_id = o.id ORDER BY order_date ASC, cost DESC;
