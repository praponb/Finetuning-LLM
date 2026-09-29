SELECT account_id, txn_type FROM transactions INTERSECT SELECT account_id, txn_type FROM transactions WHERE account_id IN (2, 22);
