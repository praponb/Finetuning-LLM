SELECT ROUND((100 * SUM(CASE WHEN account_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_credit, MIN(balance) AS min_balance
FROM accounts
WHERE status = 'OPEN';
