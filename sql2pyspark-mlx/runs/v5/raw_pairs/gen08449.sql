SELECT id, amount, merchant, txn_time FROM transactions WHERE NOT (txn_type = 'CREDIT' AND amount < 3100);
