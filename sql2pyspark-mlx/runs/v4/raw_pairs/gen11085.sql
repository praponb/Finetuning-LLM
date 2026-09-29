SELECT balance, LOWER(status) AS status_lower
FROM accounts
WHERE status IN ('CLOSED', 'OPEN') AND MONTH(opened_date) IN (1, 3, 5)
