SELECT t.txn_time, a.account_type, c.email, c.credit_limit FROM transactions t LEFT OUTER JOIN accounts a ON t.account_id = a.id FULL OUTER JOIN customers c ON a.customer_id = c.id
