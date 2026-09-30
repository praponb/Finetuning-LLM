SELECT id, order_id, status FROM payments WHERE paid_at >= '2023-09-01' UNION SELECT id, order_id, status FROM payments WHERE status NOT IN ('REFUNDED', 'SUCCESS')
