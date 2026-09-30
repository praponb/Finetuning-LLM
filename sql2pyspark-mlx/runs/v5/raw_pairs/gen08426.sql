SELECT id, method, amount, order_id FROM payments WHERE status = 'REFUNDED' AND TO_DATE(paid_at) >= '2024-05-24';
