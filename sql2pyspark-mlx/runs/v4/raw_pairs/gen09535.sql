SELECT id, status, paid_at, RANK() OVER (PARTITION BY status ORDER BY paid_at) AS rnk FROM payments;
