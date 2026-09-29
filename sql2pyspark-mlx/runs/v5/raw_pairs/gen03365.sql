SELECT orders.id, orders.shipping_fee, orders.status FROM orders WHERE NOT EXISTS (SELECT 1 FROM payments WHERE payments.order_id = orders.id AND payments.amount BETWEEN 1100 AND 1290)
