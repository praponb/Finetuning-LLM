SELECT id, order_id, paid_at, amount, amount - LAG(amount, 1) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS amount_change FROM payments;
