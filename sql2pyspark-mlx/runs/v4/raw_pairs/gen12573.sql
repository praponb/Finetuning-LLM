SELECT payments.id, payments.method, payments.status
FROM payments
WHERE payments.order_id NOT IN (SELECT id FROM orders WHERE amount > 450)
