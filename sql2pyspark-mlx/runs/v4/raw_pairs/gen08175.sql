SELECT o.id, o.amount, o.store_id, o.channel FROM orders o WHERE o.store_id NOT IN (SELECT id FROM stores WHERE sqft = 8335) ORDER BY o.channel ASC, store_id ASC;
