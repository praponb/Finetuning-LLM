SELECT id, paid_at, SUBSTRING(status, 1, 2) AS status_prefix
FROM payments
WHERE NOT (order_id IS NOT NULL AND status IN ('REFUNDED', 'FAILED', 'SUCCESS'))
