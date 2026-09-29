SELECT id, balance, ADD_MONTHS(opened_date, 12) AS opened_date_plus_months
FROM accounts
WHERE opened_date > '2025-01-25'
ORDER BY id DESC
LIMIT 50
