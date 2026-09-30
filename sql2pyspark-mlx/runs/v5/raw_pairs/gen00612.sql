SELECT id, method, paid_at
FROM payments
WHERE NOT (paid_at >= '2024-11-07' AND order_id = 37)
