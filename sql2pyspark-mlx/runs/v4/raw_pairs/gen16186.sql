SELECT a.account_type, a.customer_id, t.txn_time FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id ORDER BY a.customer_id
