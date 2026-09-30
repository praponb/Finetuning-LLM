SELECT id, method, paid_at, RANK() OVER (PARTITION BY method ORDER BY paid_at) AS rnk FROM payments WHERE amount <= 1420;
