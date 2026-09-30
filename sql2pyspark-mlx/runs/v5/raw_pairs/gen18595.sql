SELECT t.id, t.merchant, t.txn_time, a.customer_id, a.balance, c.city, c.email FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id JOIN customers c ON a.customer_id = c.id;
