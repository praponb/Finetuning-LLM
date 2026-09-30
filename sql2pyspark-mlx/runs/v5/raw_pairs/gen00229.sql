SELECT a.id, a.customer_id, a.balance, a.account_type FROM accounts a WHERE a.customer_id IS NULL AND a.id IN (SELECT account_id FROM transactions);
