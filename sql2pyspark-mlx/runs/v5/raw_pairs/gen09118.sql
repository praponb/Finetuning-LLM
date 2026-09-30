SELECT account_type, ROUND(STDDEV(balance), 2) AS stddev_balance FROM accounts GROUP BY account_type HAVING ROUND(STDDEV(balance), 2) < 34 ORDER BY stddev_balance DESC
