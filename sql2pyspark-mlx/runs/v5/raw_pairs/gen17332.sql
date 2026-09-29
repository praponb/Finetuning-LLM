SELECT status, method FROM payments WHERE paid_at < '2025-06-18' EXCEPT SELECT status, method FROM payments WHERE status = 'FAILED' ORDER BY status
