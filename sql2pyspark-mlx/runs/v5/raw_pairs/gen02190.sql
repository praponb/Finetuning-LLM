SELECT account_type, ROUND((100 * SUM(CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_checking
FROM accounts
WHERE status IN ('FROZEN', 'CLOSED', 'OPEN') AND opened_date BETWEEN '2024-01-01' AND '2024-04-18' AND balance <= 20000
GROUP BY account_type
HAVING ROUND((100 * SUM(CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END)) / COUNT(*), 1) > 61;
