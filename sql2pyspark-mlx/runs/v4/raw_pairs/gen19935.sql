SELECT account_type, COUNT(balance) AS balance_count, COUNT(*) AS account_count
FROM accounts
WHERE account_type IN ('SAVINGS', 'CREDIT') AND status IN ('CLOSED', 'OPEN')
GROUP BY account_type
HAVING SUM(balance) > 330000
ORDER BY account_type;
