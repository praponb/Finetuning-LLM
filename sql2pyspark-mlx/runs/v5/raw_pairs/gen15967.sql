SELECT id, channel, amount, COALESCE(discount, 0) AS discount_filled FROM orders WHERE status = 'SHIPPED' AND channel != 'web';
