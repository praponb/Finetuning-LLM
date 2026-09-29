SELECT id, paid_at FROM payments WHERE status IN ('SUCCESS', 'REFUNDED', 'FAILED')
