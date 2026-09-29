WITH recent_accounts AS (
    SELECT *
    FROM accounts
    WHERE account_type != 'CREDIT'
),
accounts_stats AS (
    SELECT status, account_type, ROUND(AVG(balance), 2) AS avg_balance
    FROM recent_accounts
    GROUP BY status, account_type
)
SELECT *
FROM accounts_stats
WHERE avg_balance > 61
ORDER BY account_type;
