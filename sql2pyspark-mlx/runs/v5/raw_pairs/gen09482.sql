SELECT id, balance, CONCAT(status, ' ', account_type) AS status_account_type, MONTH(opened_date) AS opened_date_month FROM accounts ORDER BY opened_date_month, balance DESC;
