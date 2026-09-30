SELECT a.balance, a.customer_id, t.account_id, t.txn_type FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id
