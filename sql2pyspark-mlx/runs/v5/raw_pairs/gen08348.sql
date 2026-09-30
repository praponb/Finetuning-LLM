SELECT id, method, amount, paid_at, status FROM payments QUALIFY ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY amount DESC NULLS FIRST, id) = 1;
