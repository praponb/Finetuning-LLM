SELECT status
FROM payments
WHERE method IN ('card', 'promptpay', 'bank_transfer')
UNION ALL
SELECT status
FROM shipments
WHERE order_id IS NOT NULL
ORDER BY status
