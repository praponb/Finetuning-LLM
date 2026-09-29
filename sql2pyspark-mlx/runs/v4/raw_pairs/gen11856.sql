SELECT o.id, o.store_id FROM orders o WHERE o.channel IN ('store', 'web') AND o.store_id IN (SELECT id FROM stores WHERE city IN ('Singapore', 'Berlin'))
