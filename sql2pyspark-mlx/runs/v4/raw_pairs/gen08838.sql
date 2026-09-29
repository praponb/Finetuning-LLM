SELECT a.id, a.account_type, a.balance, a.opened_date FROM accounts a WHERE a.balance > (SELECT AVG(b.balance) FROM accounts b WHERE b.account_type = a.account_type)
