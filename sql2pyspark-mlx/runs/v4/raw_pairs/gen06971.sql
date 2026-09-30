SELECT account_id, txn_type, UPPER(txn_type) AS txn_type_upper FROM transactions WHERE account_id IN (21, 34, 38) AND txn_type IN ('FEE', 'CREDIT', 'DEBIT') ORDER BY txn_type_upper;
