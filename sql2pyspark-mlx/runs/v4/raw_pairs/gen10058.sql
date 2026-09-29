SELECT txn_type, account_id FROM transactions EXCEPT ALL SELECT txn_type, account_id FROM transactions WHERE merchant IS NULL
