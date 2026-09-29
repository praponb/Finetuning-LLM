WITH src AS (
    SELECT *
    FROM accounts
    WHERE opened_date BETWEEN '2024-12-01' AND '2025-07-17'
),
accounts_stats AS (
    SELECT account_type, MAX(balance) AS max_balance, MAX(balance) - MIN(balance) AS balance_range
    FROM src
    GROUP BY account_type
)
SELECT *
FROM accounts_stats
ORDER BY balance_range
