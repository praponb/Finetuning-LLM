SELECT id, balance, account_type, opened_date FROM accounts WHERE balance < (SELECT MIN(balance) FROM accounts);
