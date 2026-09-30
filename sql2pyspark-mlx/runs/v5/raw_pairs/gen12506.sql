SELECT id, txn_type, amount FROM transactions WHERE HOUR(txn_time) BETWEEN 8 AND 11 OR txn_type = 'CREDIT' ORDER BY id DESC
