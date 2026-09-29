SELECT id, status, paid_at, RANK() OVER (PARTITION BY status ORDER BY paid_at DESC) AS paid_at_rank FROM payments WHERE status NOT IN ('REFUNDED', 'SUCCESS');
