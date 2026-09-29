SELECT id, opened_date, balance, account_type FROM accounts WHERE opened_date >= (SELECT MAX(opened_date) FROM accounts) ORDER BY balance DESC, id LIMIT 3;
