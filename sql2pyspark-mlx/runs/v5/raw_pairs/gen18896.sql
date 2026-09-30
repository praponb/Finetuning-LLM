SELECT a.balance, a.account_type, a.customer_id, t.merchant, t.account_id, t.txn_type FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id WHERE MONTH(a.opened_date) IN (2, 7, 12)
