SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, ROUND(STDDEV(balance), 2) AS stddev_balance FROM accounts GROUP BY opened_month;
