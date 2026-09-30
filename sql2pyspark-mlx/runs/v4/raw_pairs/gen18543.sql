SELECT orders.id, orders.discount, orders.shipping_fee, orders.amount FROM orders WHERE orders.customer_id NOT IN (SELECT id FROM customers WHERE is_active)
