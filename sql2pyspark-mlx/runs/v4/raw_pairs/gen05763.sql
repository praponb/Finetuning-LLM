SELECT id, txn_type FROM transactions WHERE account_id IN (2, 35) UNION SELECT id, txn_type FROM transactions WHERE txn_time < '2023-08-09';
