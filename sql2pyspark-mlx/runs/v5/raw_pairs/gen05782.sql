SELECT id, order_id, amount, paid_at FROM payments QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at, id) = 1
