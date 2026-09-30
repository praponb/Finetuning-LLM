SELECT id, opened_date, LEAD(opened_date, 1) OVER (ORDER BY opened_date, id) AS next_opened_date FROM accounts
