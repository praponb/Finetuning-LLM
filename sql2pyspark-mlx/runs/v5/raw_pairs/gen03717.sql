SELECT id, txn_type, account_id, txn_time, amount, LAG(amount, 1) OVER (PARTITION BY txn_type, account_id ORDER BY txn_time, id) AS prev_amount FROM transactions WHERE NOT is_flagged ORDER BY amount;
