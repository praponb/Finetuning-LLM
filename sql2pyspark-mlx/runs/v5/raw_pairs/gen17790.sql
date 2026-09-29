SELECT status, COUNT(*) AS cnt, AVG(balance) AS avg_balance FROM accounts GROUP BY status;
