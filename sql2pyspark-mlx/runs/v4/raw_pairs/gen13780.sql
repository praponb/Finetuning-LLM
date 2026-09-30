SELECT *, CAST(balance AS INT) AS balance_int
FROM accounts
WHERE opened_date < '2025-03-11'
