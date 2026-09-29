SELECT account_type, MIN(opened_date) AS min_opened_date, COUNT(*) AS account_count
FROM accounts
GROUP BY account_type
