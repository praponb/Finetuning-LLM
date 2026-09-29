SELECT o.id, o.status, o.shipping_fee, o.order_date FROM orders o WHERE o.store_id NOT IN (SELECT id FROM stores WHERE region = 'East')
