SELECT a.id, a.account_type, COUNT(t.id) AS transaction_count FROM accounts a LEFT OUTER JOIN transactions t ON t.account_id = a.id WHERE t.txn_time >= '2024-07-05' GROUP BY a.id, a.account_type
