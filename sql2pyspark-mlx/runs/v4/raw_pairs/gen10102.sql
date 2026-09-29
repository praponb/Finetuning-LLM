SELECT id, merchant, txn_time, amount, amount - LAG(amount, 1, 0) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS amount_change FROM transactions WHERE amount <= 2000;
