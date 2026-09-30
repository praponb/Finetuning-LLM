SELECT status, MAX(balance) AS max_balance, SUM(balance) AS total_balance
FROM accounts
GROUP BY status;
