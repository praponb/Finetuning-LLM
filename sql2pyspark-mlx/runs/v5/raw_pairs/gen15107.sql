SELECT * FROM transactions WHERE txn_type IN ('FEE', 'REFUND', 'CREDIT') AND account_id IN (4, 11, 32, 39) ORDER BY id DESC
