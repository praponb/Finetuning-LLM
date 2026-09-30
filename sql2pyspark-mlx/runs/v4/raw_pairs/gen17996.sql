SELECT payments.id, payments.paid_at, payments.method, orders.shipping_fee, orders.store_id FROM payments JOIN orders ON payments.order_id = orders.id WHERE orders.customer_id IN (17, 22, 25, 26);
