SELECT a.customer_id, a.balance, t.txn_time, t.amount, t.account_id FROM accounts a JOIN transactions t ON t.account_id = a.id WHERE a.status = 'CLOSED'
