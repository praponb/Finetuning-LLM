SELECT id, txn_time, DATEDIFF(CURRENT_DATE(), txn_time) AS days_since_txn_time FROM transactions WHERE NOT (txn_type = 'CREDIT' OR account_id IN (8, 22)) ORDER BY txn_time DESC, id LIMIT 1
