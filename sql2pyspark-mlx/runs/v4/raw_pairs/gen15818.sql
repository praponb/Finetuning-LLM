SELECT id, method, status, paid_at FROM payments WHERE amount > 1550 AND status NOT IN ('FAILED', 'SUCCESS', 'REFUNDED') ORDER BY paid_at ASC
