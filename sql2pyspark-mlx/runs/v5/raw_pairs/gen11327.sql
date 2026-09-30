SELECT o.id, o.status FROM orders o WHERE o.shipping_fee BETWEEN 28 AND 30 AND o.store_id IN (SELECT id FROM stores WHERE region = 'North');
