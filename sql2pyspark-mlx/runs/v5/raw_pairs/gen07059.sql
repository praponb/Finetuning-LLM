SELECT t.txn_time, t.account_id, a.status, a.customer_id FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id;
