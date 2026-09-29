SELECT a.opened_date, a.account_type, t.txn_type FROM accounts a INNER JOIN transactions t ON t.account_id = a.id WHERE a.customer_id = 6 ORDER BY account_type DESC;
