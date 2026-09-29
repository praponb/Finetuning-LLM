SELECT o.id, o.status FROM orders o WHERE o.store_id IN (9, 10, 21, 32) AND o.store_id NOT IN (SELECT id FROM stores WHERE region = 'South');
