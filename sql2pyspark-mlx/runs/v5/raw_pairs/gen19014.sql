SELECT o.id, o.shipping_fee, o.customer_id FROM orders o WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id);
