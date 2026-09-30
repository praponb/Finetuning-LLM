SELECT status, AVG(balance) AS avg_balance, AVG(balance) AS avg_balance_2 FROM accounts GROUP BY status ORDER BY status DESC
