SELECT paid_at, status, amount, CAST(amount AS INT) AS amount_int FROM payments WHERE NOT (status IN ('FAILED', 'REFUNDED') AND amount > 510 AND order_id = 24)
