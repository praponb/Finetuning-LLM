SELECT id, txn_time, txn_type, CAST(amount AS INT) AS amount_int, TRIM(txn_type) AS clean_txn_type FROM transactions WHERE txn_type IN ('FEE', 'REFUND') ORDER BY txn_type DESC, id LIMIT 10;
