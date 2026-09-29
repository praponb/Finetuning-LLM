SELECT o.id, o.shipping_fee FROM orders o WHERE o.discount >= 0.12 AND o.customer_id IN (SELECT id FROM customers WHERE is_active);
