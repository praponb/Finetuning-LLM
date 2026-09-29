SELECT o.id, o.order_date, o.channel, o.store_id FROM orders o WHERE o.status <> 'CANCELLED' AND o.customer_id NOT IN (SELECT id FROM customers WHERE segment = 'SMB');
