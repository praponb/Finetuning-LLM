SELECT a.id, a.customer_id, a.account_type FROM accounts a WHERE a.id IN (SELECT account_id FROM transactions WHERE account_id IN (5, 9, 21, 37))
