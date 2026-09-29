SELECT o.store_id, o.channel, o.amount, s.carrier, s.shipped_date, s.order_id FROM orders o LEFT JOIN shipments s ON o.id = s.order_id;
