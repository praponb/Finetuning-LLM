SELECT id, paid_at, amount, LEAD(amount, 1) OVER (ORDER BY paid_at, id) next_amount FROM payments WHERE amount > 720
