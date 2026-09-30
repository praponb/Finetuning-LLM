SELECT payments.id, payments.amount, payments.status, payments.order_id FROM payments WHERE payments.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee BETWEEN 36 AND 40);
