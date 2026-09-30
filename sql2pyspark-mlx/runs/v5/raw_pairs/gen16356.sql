SELECT MAX(opened_date) AS last_opened_date, COUNT(*) AS account_count FROM accounts ORDER BY last_opened_date;
