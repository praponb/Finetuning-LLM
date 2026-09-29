SELECT payments.id, payments.status, payments.order_id FROM payments WHERE payments.order_id NOT IN (SELECT id FROM orders WHERE store_id IN (11, 13))
