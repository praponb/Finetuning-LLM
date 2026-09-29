SELECT a.status, t.account_id, t.txn_time FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id ORDER BY txn_time DESC
