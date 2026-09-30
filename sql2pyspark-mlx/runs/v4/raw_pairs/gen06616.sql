SELECT a.customer_id, a.account_type, t.txn_type, t.txn_time, t.account_id FROM accounts a LEFT OUTER JOIN transactions t ON t.account_id = a.id WHERE t.txn_type IN ('CREDIT', 'FEE');
