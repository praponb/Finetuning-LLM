SELECT id, opened_date FROM accounts WHERE opened_date = (SELECT MIN(opened_date) FROM accounts) ORDER BY opened_date DESC, id LIMIT 1;
