SELECT id, account_id, merchant FROM transactions WHERE txn_type = 'CREDIT' UNION SELECT id, account_id, merchant FROM transactions WHERE txn_type = 'CREDIT';
