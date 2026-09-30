SELECT payments.id, payments.paid_at, payments.status
FROM payments
WHERE payments.order_id IN (9, 38) AND payments.order_id IN (SELECT id FROM orders WHERE status = 'CANCELLED')
ORDER BY payments.status ASC, id ASC
