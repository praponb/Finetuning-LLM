SELECT a.id, a.status FROM accounts a WHERE a.id IN (SELECT account_id FROM transactions)
