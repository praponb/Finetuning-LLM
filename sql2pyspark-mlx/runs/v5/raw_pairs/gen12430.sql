SELECT id, order_id, paid_at, amount, SUM(amount) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount FROM payments ORDER BY id;
