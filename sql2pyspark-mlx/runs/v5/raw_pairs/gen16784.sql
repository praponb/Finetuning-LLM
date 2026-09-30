SELECT id, paid_at, RANK() OVER (ORDER BY paid_at) AS paid_at_rank
FROM payments
WHERE order_id IS NULL;
