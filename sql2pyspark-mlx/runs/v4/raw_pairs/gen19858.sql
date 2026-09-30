SELECT a.id, a.account_type, a.balance, a.opened_date FROM accounts a WHERE a.balance > (SELECT AVG(a2.balance) FROM accounts a2 WHERE a2.account_type = a.account_type)
