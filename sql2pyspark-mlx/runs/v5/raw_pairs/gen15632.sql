SELECT txn_type FROM transactions WHERE amount <= 1100 INTERSECT SELECT txn_type FROM transactions WHERE txn_type IN ('REFUND', 'CREDIT');
