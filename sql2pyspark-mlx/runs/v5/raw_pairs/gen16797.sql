SELECT a.status, t.txn_type, t.txn_time FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id;
