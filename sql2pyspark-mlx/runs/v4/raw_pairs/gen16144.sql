SELECT * FROM transactions WHERE TO_DATE(txn_time) >= '2025-01-01' ORDER BY account_id DESC, id LIMIT 10
