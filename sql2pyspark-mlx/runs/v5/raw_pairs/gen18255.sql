SELECT payments.id, payments.order_id, payments.status, payments.paid_at, orders.channel, orders.store_id, orders.shipping_fee FROM payments FULL OUTER JOIN orders ON orders.id = payments.order_id
