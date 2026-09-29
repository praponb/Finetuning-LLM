SELECT id, balance, opened_date FROM accounts WHERE balance > (SELECT MAX(balance) FROM accounts WHERE balance BETWEEN 29000 AND 61000) ORDER BY balance, id DESC LIMIT 50;
