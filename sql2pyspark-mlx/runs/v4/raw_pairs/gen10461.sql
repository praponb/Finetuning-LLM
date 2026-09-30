SELECT status, DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label
FROM accounts
WHERE account_type = 'LOAN'
ORDER BY status;
