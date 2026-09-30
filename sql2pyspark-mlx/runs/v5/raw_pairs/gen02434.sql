SELECT id, status, paid_at, amount, LEAD(amount, 1, 0) OVER (PARTITION BY status ORDER BY paid_at, id) AS next_amount FROM payments ORDER BY amount DESC, paid_at DESC
