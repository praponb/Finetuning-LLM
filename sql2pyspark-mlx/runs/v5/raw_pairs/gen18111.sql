SELECT id, opened_date, balance
FROM accounts
WHERE opened_date >= (SELECT MAX(opened_date) FROM accounts WHERE opened_date < '2025-06-01')
ORDER BY id;
