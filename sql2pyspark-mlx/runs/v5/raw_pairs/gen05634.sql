SELECT account_id, txn_type FROM transactions INTERSECT ALL SELECT account_id, txn_type FROM transactions WHERE merchant = 'Starbucks'
