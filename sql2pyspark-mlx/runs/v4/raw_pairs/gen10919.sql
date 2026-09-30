SELECT id, merchant, account_id FROM transactions WHERE (txn_type IN ('REFUND', 'FEE', 'CREDIT') AND merchant IN ('Amazon', '7-Eleven')) OR account_id IN (10, 34);
