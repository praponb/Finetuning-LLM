SELECT status, MAX(paid_at) latest_paid_at FROM payments GROUP BY status ORDER BY latest_paid_at, status
