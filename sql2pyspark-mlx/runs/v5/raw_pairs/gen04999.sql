SELECT id, txn_time, amount, FIRST_VALUE(amount) OVER (ORDER BY txn_time, id) AS first_amount FROM transactions WHERE account_id IN (18, 30, 33)
