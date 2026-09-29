SELECT o.id, o.shipping_fee, o.discount FROM orders o WHERE EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.order_id IS NULL);
