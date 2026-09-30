SELECT id, order_id FROM payments WHERE paid_at >= '2024-07-01' UNION SELECT id, order_id FROM payments WHERE status IN ('FAILED', 'REFUNDED', 'SUCCESS') ORDER BY id;
