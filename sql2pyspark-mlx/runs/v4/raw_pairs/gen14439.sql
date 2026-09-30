SELECT id, opened_date, status FROM accounts WHERE opened_date <= (SELECT MIN(opened_date) FROM accounts) ORDER BY status ASC
