SELECT balance, account_type, DATE_SUB(opened_date, 30) AS opened_date_minus_30d
FROM accounts
WHERE YEAR(opened_date) >= 2024
