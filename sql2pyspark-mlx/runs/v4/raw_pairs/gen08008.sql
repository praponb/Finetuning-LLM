SELECT account_type, MIN(opened_date) AS first_opened_date, ROUND((100 * SUM(CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_checking
FROM accounts
WHERE account_type <> 'SAVINGS' OR account_type = 'CHECKING'
GROUP BY account_type
HAVING pct_checking > 45
ORDER BY account_type DESC, first_opened_date;
