SELECT merchant FROM transactions WHERE account_id IN (4, 29, 32, 33) UNION SELECT merchant FROM transactions WHERE txn_type NOT IN ('CREDIT', 'REFUND', 'FEE');
