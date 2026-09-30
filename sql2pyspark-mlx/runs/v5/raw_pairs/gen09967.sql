SELECT o.id, o.store_id, o.channel, o.discount FROM orders o WHERE o.store_id = 14 AND o.customer_id IN (SELECT id FROM customers WHERE country <> 'UK') ORDER BY discount DESC, id ASC LIMIT 50;
