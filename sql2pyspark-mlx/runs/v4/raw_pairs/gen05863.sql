SELECT o.id, o.channel, o.store_id FROM orders o WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.method = 'bank_transfer') ORDER BY id LIMIT 5;
