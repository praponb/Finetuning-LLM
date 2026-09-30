SELECT merchant, account_id FROM transactions WHERE txn_type = 'REFUND' EXCEPT SELECT merchant, account_id FROM transactions WHERE merchant = 'Shell'
