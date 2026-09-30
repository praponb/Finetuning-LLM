SELECT a.account_type, a.status, a.customer_id, t.account_id, t.txn_time, t.merchant FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id WHERE t.account_id IN (18, 26, 28);
