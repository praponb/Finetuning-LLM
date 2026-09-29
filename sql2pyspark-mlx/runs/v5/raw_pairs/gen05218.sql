SELECT account_type, ROUND(AVG(balance), 2) AS avg_balance, ROUND(STDDEV(balance), 2) AS stddev_balance FROM accounts GROUP BY account_type;
