SELECT id, account_id, txn_time, amount, SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS running_amount FROM transactions ORDER BY account_id
