SELECT payments.id, payments.order_id, payments.status
FROM payments
WHERE payments.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee <= 31)
ORDER BY payments.id DESC;
