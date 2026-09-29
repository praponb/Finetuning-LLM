SELECT status, method, paid_at, CAST(amount AS INT) AS amount_int FROM payments
