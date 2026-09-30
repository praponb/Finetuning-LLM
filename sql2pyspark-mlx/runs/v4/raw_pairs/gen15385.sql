SELECT
    status,
    account_type,
    MAX(balance) AS max_balance,
    MIN(opened_date) AS min_opened_date
FROM accounts
GROUP BY 1, 2
HAVING MAX(balance) < 38000
ORDER BY status, max_balance DESC;
