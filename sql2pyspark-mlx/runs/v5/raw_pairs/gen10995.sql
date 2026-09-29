SELECT a.balance, a.account_type, t.txn_time, t.merchant, t.account_id FROM accounts a LEFT OUTER JOIN transactions t ON a.id = t.account_id
