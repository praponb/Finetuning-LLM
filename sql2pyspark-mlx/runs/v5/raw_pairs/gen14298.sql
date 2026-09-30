SELECT a.id, a.balance, a.opened_date FROM accounts a WHERE a.id IN (SELECT account_id FROM transactions);
