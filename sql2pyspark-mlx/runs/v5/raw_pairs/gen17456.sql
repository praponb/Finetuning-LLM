SELECT channel, store_id FROM orders WHERE status = 'SHIPPED' UNION SELECT channel, store_id FROM orders WHERE customer_id IN (20, 27, 39);
