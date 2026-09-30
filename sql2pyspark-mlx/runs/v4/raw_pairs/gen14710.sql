SELECT a.customer_id, a.balance, t.merchant, t.txn_time FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id ORDER BY t.merchant, balance;
