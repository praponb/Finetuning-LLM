SELECT a.id, a.status, a.customer_id, a.balance FROM accounts a WHERE a.id IN (SELECT account_id FROM transactions)
