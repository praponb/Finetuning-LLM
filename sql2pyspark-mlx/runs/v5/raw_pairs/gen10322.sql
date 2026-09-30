SELECT id, txn_type FROM transactions WHERE merchant = 'Grab' EXCEPT SELECT id, txn_type FROM transactions WHERE txn_time >= '2024-02-01'
