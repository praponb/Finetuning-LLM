SELECT a.id, a.account_type, a.balance FROM accounts a WHERE a.balance = (SELECT MAX(b.balance) FROM accounts b WHERE b.account_type = a.account_type);
