SELECT id, merchant, txn_type FROM transactions WHERE merchant = '7-Eleven' UNION SELECT id, merchant, txn_type FROM transactions WHERE account_id IN (23, 30, 39)
