SELECT id, order_id
FROM payments
WHERE amount >= 1560 OR status IN ('FAILED', 'REFUNDED')
ORDER BY order_id DESC, id DESC
