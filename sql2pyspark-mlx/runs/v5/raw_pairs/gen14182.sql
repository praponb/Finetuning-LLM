SELECT txn_type, txn_time, CASE WHEN txn_type = 'DEBIT' THEN 1 ELSE 0 END AS is_debit FROM transactions WHERE amount IS NOT NULL ORDER BY txn_time, txn_type
