SELECT id, status, order_id, amount, DENSE_RANK() OVER (PARTITION BY status, order_id ORDER BY amount DESC) AS amount_dense_rank FROM payments WHERE method IN ('bank_transfer', 'cash', 'card')
