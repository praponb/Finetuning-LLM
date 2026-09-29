SELECT account_type, AVG(balance) avg_balance FROM accounts GROUP BY account_type HAVING AVG(balance) >= 28000 ORDER BY avg_balance, account_type DESC LIMIT 5;
