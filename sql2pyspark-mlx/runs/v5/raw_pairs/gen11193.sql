SELECT a.id, a.customer_id FROM accounts a WHERE a.id IN (SELECT account_id FROM transactions)
