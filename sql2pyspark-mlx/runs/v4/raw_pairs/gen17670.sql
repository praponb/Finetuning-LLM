SELECT a.status, a.opened_date, a.customer_id, t.txn_time, t.account_id FROM accounts a INNER JOIN transactions t ON a.id = t.account_id WHERE t.txn_type IN ('CREDIT', 'FEE')
