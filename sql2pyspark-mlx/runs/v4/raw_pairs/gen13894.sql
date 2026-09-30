SELECT id, status, COALESCE(amount, 0) amount_filled, CAST(amount AS INT) amount_int FROM payments WHERE order_id = 2 ORDER BY id DESC;
