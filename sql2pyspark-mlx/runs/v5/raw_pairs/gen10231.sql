WITH src AS (
    SELECT *
    FROM accounts
    WHERE YEAR(opened_date) = 2025
),
accounts_stats AS (
    SELECT account_type, MAX(balance) - MIN(balance) AS balance_range
    FROM src
    GROUP BY account_type
)
SELECT account_type, balance_range
FROM accounts_stats
WHERE balance_range < 77;
