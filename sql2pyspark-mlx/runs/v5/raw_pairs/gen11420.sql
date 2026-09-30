SELECT YEAR(opened_date) AS opened_year, account_type, MIN(balance) AS min_balance, ROUND(STDDEV(balance), 2) AS stddev_balance FROM accounts GROUP BY YEAR(opened_date), account_type
