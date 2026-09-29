SELECT id, account_id, amount, DENSE_RANK() OVER (PARTITION BY account_id ORDER BY amount) AS amount_dense_rank FROM transactions ORDER BY amount DESC
