SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, SUM(balance) AS total_balance, MAX(opened_date) AS last_opened_date FROM accounts GROUP BY DATE_TRUNC('MONTH', opened_date)
