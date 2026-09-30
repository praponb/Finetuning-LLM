SELECT status, SUM(balance) AS total_balance, MIN(balance) AS min_balance, AVG(balance) AS avg_balance FROM accounts GROUP BY status ORDER BY 2 DESC, status LIMIT 5
